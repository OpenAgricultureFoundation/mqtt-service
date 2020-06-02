#!/usr/bin/env python3

""" This service subscribes for MQTT messages published by our devices.
    - Keeps a cache of data in the datastore.DeviceData<device_ID>.<var>
    - Saves data to bigquery.
    - Publishes notification messages.
"""

import os, sys, json, argparse, logging, signal, traceback
import paho.mqtt.client as mqtt

# from cloud_common.cc import version as cc_version
# from cloud_common.cc.google import pubsub # takes a few secs...
# from cloud_common.cc.google import env_vars
from cloud_common.cc.mqtt.local_mqtt_messaging import MQTTMessaging


#------------------------------------------------------------------------------
# This is for old Google based pubsub
# Sadly, we have to use a global class instance here, since the pubsub message
# callback signature won't let me call a class method.
# mqtt_messaging = MQTTMessaging()

mqtt_messaging = None # this will be setup in __main__()


#------------------------------------------------------------------------------
# Handle the user pressing Control-C
def signal_handler(signal, frame):
    logging.critical( 'Exiting.' )
    sys.exit(0)
signal.signal( signal.SIGINT, signal_handler )


#------------------------------------------------------------------------------
# This callback is called for each message we receive on the device events 
# pubsub topic.
# We acknowledge the message, then validate and act on it if valid.
def callback(mqttc, userdata, msg):
    try:
        #msg.ack() # acknowledge to the server that we got the message
        logging.error("RECIVED MESSAGE")
        split_topic = msg.topic.split("/")
        if len(split_topic) < 2:
            logging.error(f'Can not get deviceId from topic: ' + msg.topic)
            return
        deviceId = split_topic[2]

        #if len(msg.data) == 0:
        #    logging.error(f'No data in message.')
        #    return

        display_data = msg.payload
        if 250 < len(display_data):
            display_data = f"... {len(display_data)} bytes ..."
        logging.error(f'subs callback received:\n\n{display_data}\nfrom {deviceId}\n')

        # try to decode the byte data as a string / JSON, exception if not json
        pydict = json.loads(msg.payload.decode('utf-8'))

        # finally let our mqtt class parse this message and decide how to 
        # handle it
        if userdata['mqtt_messaging'] is not None:
            userdata['mqtt_messaging'].parse(deviceId, pydict)
        else:
            logging.error('No mqtt_messaging defined while in callback')

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(f'Exception in callback(): {e}')
        traceback.print_tb( exc_traceback, file=sys.stdout )


#------------------------------------------------------------------------------
def main():

    # Default log level.
    logging.basicConfig(level=logging.ERROR) # can only call once

    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, 
        help='log level: debug, info, warning, error, critical', 
        default='info' )
    parser.add_argument('--host', type=str,
                        help='Set a local mqtt broker host, default `mqtt`',
                        default='mqtt')
    parser.add_argument('--port', type=int,
                        help='Set a local mqtt broker port, default 1883',
                        default=1883)
    parser.add_argument('--name', type=str,
                        help='Set the mqtt client name, defaults to openag_mqtt_service',
                        default='openag_mqtt_service')
    parser.add_argument('--db_host', type=str,
                        help='Set a local influxdb host, default `influxdb`',
                        default='influxdb')
    parser.add_argument('--db_port', type=int,
                        help='Set a local influxdb port, default 8086',
                        default=8086)
    parser.add_argument('--db_name', type=str,
                        help='Set the influx DB name, defaults to openag_local',
                        default='openag_local')
    args = parser.parse_args()

    # User specified log level.
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int ):
        logging.critical(f'publisher: Invalid log level: {args.log}')
        numeric_level = getattr(logging, 'ERROR', None)
    logging.getLogger().setLevel(level=numeric_level)

    mqtt.client_name = args.name
    mqtt_host = args.host
    mqtt_port = args.port

    db_host = args.db_host
    db_port = args.db_port
    db_name = args.db_name
    client_userdata = {'mqtt_messaging': MQTTMessaging(host=db_host, port=db_port, db_name=db_name)}

    # Make sure our env. vars are set up.
    #if None == env_vars.cloud_project_id or None == env_vars.dev_events or \
    #        None == env_vars.bq_dataset or None == env_vars.bq_table or \
    #        None == env_vars.cs_bucket or None == env_vars.cs_upload_bucket:
    #    logging.critical('Missing required environment variables.')
    #    exit(1)

    # logging.info(f'{os.path.basename(__file__)} using cloud_common version {cc_version.__version__}')

    # Infinetly re-subscribe to this topic and receive callbacks for each
    # message.
    # pubsub.subscribe(env_vars.cloud_project_id, env_vars.dev_events, callback)
    client = mqtt.Client(args.name, userdata=client_userdata)
    client.on_message = callback
    client.connect(host=mqtt_host, port=mqtt_port)

    client.subscribe("/devices/#")

    client.loop_forever()


#------------------------------------------------------------------------------
if __name__ == "__main__":
    main()



