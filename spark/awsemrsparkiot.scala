package com.forsynet.awsspark

import java.nio.ByteBuffer
import java.util.UUID

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.kinesis.KinesisInputDStream


object IotKienesisDdbImproved {

  val spark = SparkSession.builder()
    .appName("Kinesis Read")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.testing.memory",471859200)
    .config("spark.driver.memory","2g")
    .config("spark.executor.memory","2g")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val batchInterval = Seconds(5)
  val ssc = new StreamingContext(spark.sparkContext, batchInterval)
  import spark.implicits._
  spark.conf.set("spark.ui.port",4042)

  val kinesisClient = AmazonKinesisClient.builder().build()

  def iotKinesisReadStream (appName:String, streamName:String, endpointUrl:String) = {


    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    //    val kinesisClient = new AmazonKinesisClient(credentials)

    //    kinesisClient.setEndpoint(endpointUrl)

    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size
    val numStreams = numShards



    val kinesisCheckpointInterval = batchInterval
    val regionName = getRegionNameByEndpoint(endpointUrl)

    println(s"    Reading from $streamName with $numShards shards in region $regionName")


    // Create the Kinesis DStreams  create 1 Kinesis Receiver/input DStream for each shard
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .streamName(streamName)
        .checkpointAppName(appName)
        .initialPosition(new Latest())
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    val unionStreams = ssc.union(kinesisStreams)

    // Convert each line of Array[Byte] to String
//    val sensorDataJson = unionStreams.map(byteArray => new String(byteArray))
    unionStreams.map(byteArray => new String(byteArray))

  }

  def getDriverData(licenseplate:String) = {
    val url = "jdbc:redshift://rtr-iot-redshift-cluster-1.cvjdtaltj22n.us-east-2.redshift.amazonaws.com:5439/rtriot"
    val user = "awsuser"
    val iamRole = "arn:aws:iam::998297988530:role/RTR-Glue-Redshift-Full"
    val redShifTable = "public.rtr_iot_driver_data"

    // Get some data from a Redshift table
    val driverDf = spark.read
      .format("jdbc")
      .option("url",url)
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("user",user)
      .option("password","We12come")
      .option("dbtable",redShifTable )
      .load()
      .createOrReplaceTempView("driver_data")

    spark.sql( s"select first_name,last_name,licenseplate,myphone from driver_data where licenseplate = '$licenseplate'")

  }



    def processIotStreamKinesis(appName:String, streamName:String, dynamoDbName:String,endpointUrl:String ) = {

      val iotSchema = StructType(Array(
        StructField("iottimestamp", StringType),
        StructField("licensePlate", StringType),
        StructField("longitude", StringType),
        StructField("latitude", StringType),
        StructField("city", StringType),
        StructField("state", StringType),
        StructField("speed", StringType)
      ))

      val sensorDataJson = iotKinesisReadStream(appName:String, streamName:String, endpointUrl:String)
      val regionName = getRegionNameByEndpoint(endpointUrl)

      sensorDataJson.foreachRDD(rdd => {
        val ds = spark.createDataset(rdd)
        val payLoadDs = ds.select(from_json(col("value"), iotSchema).as("iot"))
          .select(col("iot.iottimestamp"),
            col("iot.licensePlate"),
            col("iot.longitude"),
            col("iot.latitude"),
            col("iot.city"),
            col("iot.state"),
            col("iot.speed").cast("integer").as("speed"))
   //     payLoadDs.show()

        val speedingVehicles = payLoadDs.filter(col("speed")>75)
        println("Reading Sensor data Speeding vehicle from iot")
        speedingVehicles.show()

        val licensePlateDF = speedingVehicles.select(col("licensePlate")).map(row => row.getString(0)).collect()
//
        licensePlateDF.foreach(plate => {
          println("Speeding plate",plate)
          val driverData = getDriverData(plate)
          val joinCondition = speedingVehicles.col("licensePlate") === driverData.col("licenseplate")
          val speedingDriver = speedingVehicles.join(driverData, joinCondition, "inner").drop(driverData.col("licenseplate"))
          println("Speeding driver details after df join")
          speedingDriver.show()
          val data = speedingDriver.toJSON.map(row => row.toString).collect()(0)
          println("Speedign Drive Data toJson ",data)
          val kinesisOutStream = "rtr-iot-kinesis-out-stream"

          // Put the record onto the kinesis stream
          val putRecordRequest = new PutRecordRequest()

          putRecordRequest.setStreamName(kinesisOutStream)
          putRecordRequest.setPartitionKey(String.valueOf(UUID.randomUUID()))
          putRecordRequest.setData(ByteBuffer.wrap(data.getBytes()))
          kinesisClient.putRecord(putRecordRequest)
        })

     // write to dynamodb
     speedingVehicles.write
       .format("com.audienceproject.spark.dynamodb.datasource")
       .option("tableName", dynamoDbName)
       .option("region",regionName)
       .save()

    })



    ssc.start()
    ssc.awaitTermination()
    }



  def main(args: Array[String]): Unit = {
    val Array(appName, streamName, dynamoDbName,endpointUrl) = args
    processIotStreamKinesis(appName, streamName, dynamoDbName,endpointUrl)


  }


}
