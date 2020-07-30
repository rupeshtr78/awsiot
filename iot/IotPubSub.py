from __future__ import absolute_import
from __future__ import print_function
import argparse
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import RPi.GPIO as GPIO
import json
from datetime import datetime
import sys
import threading
import time


parser = argparse.ArgumentParser(description="Send and receive messages through and MQTT connection.")
parser.add_argument('--endpoint', required=True)
parser.add_argument('--cert')
parser.add_argument('--key')
parser.add_argument('--root-ca')
parser.add_argument('--client-id', default='samples-client-id')
parser.add_argument('--topic', default="samples/test")
parser.add_argument('--message', default="Hello World!")
parser.add_argument('--count', default=10, type=int)
parser.add_argument('--use-websocket', default=False, action='store_true')
parser.add_argument('--signing-region', default='us-east-1')
parser.add_argument('--proxy-host')
parser.add_argument('--proxy-port', type=int, default=8080)
parser.add_argument('--verbosity', choices=[x.name for x in io.LogLevel], default=io.LogLevel.NoLogs.name)

# Using globals to simplify sample code
args = parser.parse_args()

io.init_logging(getattr(io.LogLevel, args.verbosity), 'stderr')

received_count = 0
received_all_event = threading.Event()


# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    if received_count == args.count:
        received_all_event.set()

def main():

    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    


    if args.use_websocket == True:
        proxy_options = None
        if (args.proxy_host):
            proxy_options = http.HttpProxyOptions(host_name=args.proxy_host, port=args.proxy_port)

        credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)
        mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=args.endpoint,
            client_bootstrap=client_bootstrap,
            region=args.signing_region,
            credentials_provider=credentials_provider,
            websocket_proxy_options=proxy_options,
            ca_filepath=args.root_ca,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=6)

    else:
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=args.endpoint,
            cert_filepath=args.cert,
            pri_key_filepath=args.key,
            client_bootstrap=client_bootstrap,
            ca_filepath=args.root_ca,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=6)

    print("Connecting to {} with client ID '{}'...".format(
        args.endpoint, args.client_id))

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # Subscribe
    print("Subscribing to topic '{}'...".format(args.topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=args.topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

    # Publish message to server From Raspberry Pi.
    # This step Sends Payload each time button is pressed.


    lightPin = 4
    buttonPin = 17

    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)

    lightPin = 4
    buttonPin = 17
    GPIO.setup(lightPin, GPIO.OUT,initial=GPIO.LOW)
    GPIO.setup(buttonPin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

    if args.count == 0:
        print ("Sending messages until program killed")
    else:
        print ("Sending {} message(s)".format(args.count))

    def mqttPublishLines(buttonPin):
        publish_count = 1
        filepath = "/home/pi/awsiot/mqtt/data/speeding_data.csv"
        file = open(filepath, "r")
        #line = file.readlines()
        line = file.read().splitlines()
        while (publish_count <= args.count) or (args.count == 0):
            if GPIO.input(buttonPin)==GPIO.LOW:
                payload = {"iottimestamp":str(datetime.utcnow()),
                            "licensePlate":line[publish_count].split(",")[0],
                            "longitude":line[publish_count].split(",")[1],
                            "latitude":line[publish_count].split(",")[2],
                            "city":line[publish_count].split(",")[3],
                            "state":line[publish_count].split(",")[4],
                            "speed":line[publish_count].split(",")[5] }
                message = json.dumps(payload)
                print("Button Pressed", publish_count)
                GPIO.output(lightPin, True)
                mqtt_connection.publish(
                topic=args.topic,
                payload=message,
                qos=mqtt.QoS.AT_LEAST_ONCE)
                time.sleep(1)
                publish_count += 1
            else:
                GPIO.output(lightPin, False)

    GPIO.add_event_detect(buttonPin,GPIO.BOTH,callback=mqttPublishLines,bouncetime=300)


    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
    if args.count != 0 and not received_all_event.is_set():
        print("Waiting for all messages to be received...")

    received_all_event.wait()
    print("{} message(s) received.".format(received_count))

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
   
    


if __name__ == '__main__':
    main()
    
    try:    
        print("Keyboard Interrupted!")
    except KeyboardInterrupt:
        GPIO.cleanup()
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
