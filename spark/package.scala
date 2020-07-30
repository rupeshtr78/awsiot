package com.forsynet


import java.sql.Timestamp

import scala.collection.JavaConverters._
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesis

package object awsspark {



    def getRegionNameByEndpoint(endpoint: String): String = {
      val uri = new java.net.URI(endpoint)
      RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
        .asScala
        .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
        .map(_.getName)
        .getOrElse(
          throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
    }



  case class SensorPayload(
                            iottimestamp: String,
                            licensePlate:String,
                            speed : String
                          )

  case class SensorClass(
                          iottimestamp: Timestamp,
                          licensePlate:String,
                          speed : Int
                        )

}
