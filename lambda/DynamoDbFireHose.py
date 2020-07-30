import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

firehose = boto3.client('firehose')
deliveryStreamName = 'rtr-iot-dynamodb-stream'


def lambda_handler(event, context):
    for record in event['Records']:        
        # logger.info(record)        
        ddbRecord = record['dynamodb']
        # logger.info('DDB Record: ' + json.dumps(ddbRecord))
        parsedPayload = ddbRecord['NewImage']
        # logger.info('Parsed Payload Json: ' +json.dumps(parsedPayload))
        
        iottimestamp = parsedPayload["iottimestamp"]['S']
        licensePlate = parsedPayload["licensePlate"]['S']
        longitude = parsedPayload["longitude"]['S']
        latitude = parsedPayload["latitude"]['S']
        city = parsedPayload["city"]['S']
        state = parsedPayload["state"]['S']
        speed = parsedPayload["speed"]['N']
        
        firehoseRecord ='{{"iottimestamp":"{}","licensePlate":"{}","longitude":"{}","latitude":"{}","city":"{}","state":"{}","speed":{}}}'.format(iottimestamp,licensePlate,longitude,latitude,city,state,speed)
        logger.info('Firehose Record: ' + firehoseRecord)
        
        firehose.put_record(DeliveryStreamName=deliveryStreamName, Record={ 'Data': firehoseRecord})

        
               
    return 'processed {} records.'.format(len(event['Records']))
