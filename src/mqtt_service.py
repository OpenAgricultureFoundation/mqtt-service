#!/usr/bin/env python3

""" This service subscribes for MQTT messages published by our devices.
    - Keeps a cache of data in the datastore.DeviceData<device_ID>.<var>
    - Saves data to bigquery.
    - Publishes notification messages.
"""

import os, sys, json, argparse, logging, signal, traceback

from cloud_common.cc import version as cc_version
from cloud_common.cc.google import pubsub # takes a few secs...
from cloud_common.cc.google import env_vars 
from cloud_common.cc.mqtt.mqtt_messaging import MQTTMessaging


#------------------------------------------------------------------------------
# Sadly, we have to use a global class instance here, since the pubsub message
# callback signature won't let me call a class method.
mqtt_messaging = MQTTMessaging()


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
def callback(msg):
    try:
        msg.ack() # acknowledge to the server that we got the message

        if len(msg.data) == 0:
            logging.error(f'No data in message.')
            return 

        display_data = msg.data
        if 250 < len(display_data):
            display_data = "..."
        logging.debug(f'subs callback received:\n\n{display_data}\n')

        # try to decode the byte data as a string / JSON, exception if not json
        pydict = json.loads(msg.data.decode('utf-8'))

        # finally let our mqtt class parse this message and decide how to 
        # handle it
        mqtt_messaging.parse(msg.attributes['deviceId'], pydict)

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
    args = parser.parse_args()

    # User specified log level.
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int ):
        logging.critical(f'publisher: Invalid log level: {args.log}')
        numeric_level = getattr(logging, 'ERROR', None)
    logging.getLogger().setLevel(level=numeric_level)

    # Make sure our env. vars are set up.
    if None == env_vars.cloud_project_id or None == env_vars.dev_events or \
            None == env_vars.bq_dataset or None == env_vars.bq_table or \
            None == env_vars.cs_bucket or None == env_vars.cs_upload_bucket:
        logging.critical('Missing required environment variables.')
        exit(1)

    logging.info(f'{os.path.basename(__file__)} using cloud_common version {cc_version.__version__}')

    # Infinetly re-subscribe to this topic and receive callbacks for each
    # message.
    pubsub.subscribe(env_vars.cloud_project_id, env_vars.dev_events, callback)


#------------------------------------------------------------------------------
if __name__ == "__main__":
    main()



