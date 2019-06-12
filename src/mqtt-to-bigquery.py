#!/usr/bin/env python3

""" This script reads data from the MQTT device events telemetry 
    (actaully a PubSub) topic, and stores it in:
    - BiqQuery using the BigQuery batch API.
    - Datastore (realtime doc DB, used as cache of last 100 values).
    - Storage (files, images, blobs).
"""

import os, sys, time, json, argparse, traceback, tempfile, logging, signal

from google.cloud import pubsub
from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import storage

import utils 


# globals
NUM_RETRIES = 3
BQ = None
CS = None
DS = None


#------------------------------------------------------------------------------
# Handle the user pressing Control-C
def signal_handler(signal, frame):
    logging.critical( 'Exiting.' )
    sys.exit(0)
signal.signal( signal.SIGINT, signal_handler )


#------------------------------------------------------------------------------
# This callback is called for each PubSub/IoT message we receive.
# We acknowledge the message, then validate and act on it if valid.
def callback(msg):
    try:
        msg.ack() # acknowledge to the server that we got the message

        display_data = msg.data
        if 250 < len(display_data):
            display_data = "..."
        logging.debug('data={}\n  deviceId={}\n  subFolder={}\n  '
            'deviceNumId={}\n'.
            format( 
                display_data, 
                msg.attributes['deviceId'],
                msg.attributes['subFolder'],
                msg.attributes['deviceNumId'] ))

        global CS 
        global DS 
        global BQ 

        # try to decode the byte data as a string / JSON
        pydict = json.loads( msg.data.decode('utf-8'))
        utils.save_data( CS, DS, BQ, pydict, msg.attributes['deviceId'],
            os.getenv('GCLOUD_PROJECT'),
            os.getenv('BQ_DATASET'),
            os.getenv('BQ_TABLE'),
            os.getenv('CS_BUCKET'))

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical( "Exception in callback(): %s" % e)
        traceback.print_tb( exc_traceback, file=sys.stdout )


#------------------------------------------------------------------------------
def main():

    # default log level
    logging.basicConfig( level=logging.ERROR ) # can only call once

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument( '--log', type=str, 
        help='log level: debug, info, warning, error, critical', 
        default='info' )
    args = parser.parse_args()

    # user specified log level
    numeric_level = getattr( logging, args.log.upper(), None )
    if not isinstance( numeric_level, int ):
        logging.critical('publisher: Invalid log level: %s' % \
                args.log )
        numeric_level = getattr( logging, 'ERROR', None )
    logging.getLogger().setLevel( level=numeric_level )

    # make sure our env. vars are set up
    if None == os.getenv('GCLOUD_PROJECT') or \
       None == os.getenv('GCLOUD_DEV_EVENTS') or \
       None == os.getenv('CS_BUCKET'):
        logging.critical('Missing required environment variables.')
        exit( 1 )

    # instantiate the clients we need
    global BQ 
    BQ = bigquery.Client()

    global CS 
    CS = storage.Client( project = os.getenv('GCLOUD_PROJECT'))

    global DS 
    DS = datastore.Client( os.getenv('GCLOUD_PROJECT'))

    # the resource path for the topic 
    PS = pubsub.SubscriberClient()
    subs_path = PS.subscription_path( os.getenv('GCLOUD_PROJECT'), 
                                      os.getenv('GCLOUD_DEV_EVENTS') )

    # subscribe for messages
    logging.info( 'Waiting for message sent to %s' % subs_path )

    # in case of subscription timeout, use a loop to resubscribe.
    while True:  
        try:
            future = PS.subscribe( subs_path, callback )

            # result() blocks until future is complete 
            # (when message is ack'd by server)
            message_id = future.result()
            logging.debug('\tfrom future, message_id: {}'.format(message_id))

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.critical( "Exception in main(): %s" % e)
            traceback.print_tb( exc_traceback, file=sys.stdout )


#------------------------------------------------------------------------------
if __name__ == "__main__":
    main()



