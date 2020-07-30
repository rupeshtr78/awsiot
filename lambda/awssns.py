from __future__  import print_function
import json as json
import boto3
import base64

client = boto3.client('sns',
    region_name='us-east-1'
    )
# Include your SNS topic ARN here.
topic_arn = "arn:aws:sns:us-east-1:998297988530:rtr-iot-kinesis-sns"

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data'])
        # parse payload:
        parsedPayload = json.loads(payload)

        # the result is a Python dictionary:
        iottimestamp = parsedPayload["iottimestamp"]
        first_name = parsedPayload["first_name"]
        last_name  = parsedPayload["last_name"]
        licensePlate = parsedPayload["licensePlate"]
        longitude = parsedPayload["longitude"]
        latitude = parsedPayload["latitude"]
        city = parsedPayload["city"]
        state = parsedPayload["state"]
        speed = parsedPayload["speed"]
        phoneNumber = parsedPayload["myphone"]
        e164phoneNumber = '+1'+phoneNumber
     
        message_sns = "To {} {}, \n Your vehicle with licenseplate {} was clocked with speed above the permissible limit at {},{}\n Clocked speed {} \n Geo location {}, {} \n Time {}.".format(first_name,last_name,licensePlate,city,state,speed,longitude,latitude,iottimestamp)
        print(message_sns)
        try:
            client.subscribe(TopicArn=topic_arn,Protocol='sms', Endpoint=e164phoneNumber)
            client.publish(TopicArn=topic_arn,Message=message_sns )
            print('Successfully delivered Speeding alarm message to {} '.format(phoneNumber))
        except Exception:
            print('SNS Message Delivery failure')
        
    return 'Successfully processed {} records.'.format(len(event['Records']))
