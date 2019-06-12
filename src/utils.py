#!/usr/bin/env python3

""" This file contains some utilities used for processing data and 
    writing data to Storage, Datastore and BigQuery.
"""

import os, time, logging, struct, sys, traceback, base64, ast
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import storage


# should be enough retries to insert into BQ
NUM_RETRIES = 3


# keys common to all messages
messageType_KEY = 'messageType'
messageType_EnvVar = 'EnvVar'
messageType_CommandReply = 'CommandReply'
messageType_Image = 'Image' 
messageType_ImageUpload = 'ImageUpload'

# keys for messageType='EnvVar' (and also 'CommandReply')
var_KEY = 'var'
values_KEY = 'values'

# keys for messageType='Image'
varName_KEY = 'varName'
imageType_KEY = 'imageType'
fileName_KEY = 'fileName'
#TODO all 4 keys below deprecated, remove after September 30, 2019 (or there abouts).
chunk_KEY = 'chunk'
totalChunks_KEY = 'totalChunks'
imageChunk_KEY = 'imageChunk'
messageID_KEY = 'messageID'

# keys for datastore entities
DS_device_data_KEY = 'DeviceData'
DS_env_vars_MAX_size = 100 # maximum number of values in each env. var list
DS_image_upload_queue_KEY = 'ImageUploadQueue'
DS_images_KEY = 'Images'
UPLOAD_BUCKET_NAME = 'openag-public-image-uploads'


#------------------------------------------------------------------------------
def validDictKey( d, key ):
    if key in d:
        return True
    else:
        return False


#------------------------------------------------------------------------------
# returns the messageType key if valid, else None.
def validateMessageType( valueDict ):

    if not validDictKey( valueDict, messageType_KEY ):
        logging.error('Missing key %s' % messageType_KEY )
        return None

    if messageType_EnvVar == valueDict[ messageType_KEY ]:
        return messageType_EnvVar

    if messageType_CommandReply == valueDict[ messageType_KEY ]:
        return messageType_CommandReply

    if messageType_Image == valueDict[ messageType_KEY ]:
        return messageType_Image

    if messageType_ImageUpload == valueDict[ messageType_KEY ]:
        return messageType_ImageUpload

    logging.error('validateMessageType: Invalid value {} for key {}'.format(
        valueDict[ messageType_KEY ], messageType_KEY ))
    return None


#------------------------------------------------------------------------------
# Make a BQ row that matches the table schema for the 'vals' table.
# (python will pass only mutable objects (list) by reference)
def makeBQEnvVarRowList( valueDict, deviceId, rowsList, idKey ):
    # each received EnvVar type message must have these fields
    if not validDictKey( valueDict, var_KEY ) or \
       not validDictKey( valueDict, values_KEY ):
        logging.error('makeBQEnvVarRowList: Missing key(s) in dict.')
        return

    varName = valueDict[ var_KEY ]
    values = valueDict[ values_KEY ]

    # clean / scrub / check the values.  
    deviceId = deviceId.replace( '~', '' ) 
    varName = varName.replace( '~', '' ) 

    # NEW ID format:  <KEY>~<valName>~<created UTC TS>~<deviceId>
    ID = idKey + '~{}~{}~' + deviceId

    row = ( ID.format( varName, 
        time.strftime( '%FT%XZ', time.gmtime() )), # id column
        values, 0, 0 ) # values column, with zero for X, Y

    rowsList.append( row )


#------------------------------------------------------------------------------
# returns True if there are rows to insert into BQ, false otherwise.
def makeBQRowList( valueDict, deviceId, rowsList ):

    messageType = validateMessageType( valueDict )
    if None == messageType:
        return False

    # write envVars and images (as envVars)
    if messageType_EnvVar == messageType or \
       messageType_Image == messageType:
        makeBQEnvVarRowList( valueDict, deviceId, rowsList, 'Env' )
        return True

    if messageType_CommandReply == messageType:
        makeBQEnvVarRowList( valueDict, deviceId, rowsList, 'Cmd' )
        return True

    return False

"""
example of the MQTT device telemetry message we receive:

# a packed binary image which contains its name
data=b'pascal-string-length-prefixed-camera-name, then image binary'

# JSON string image env. var.
data=b'{"messageType": "Image", "var": "webcam-top","{'values':[{'name':'URL', 'type':'str', 'value':'https://storage.googleapis.com/openag-v1-images/EDU-E40B8A78-f4-0f-24-19-fe-88_webcam-top_2018-06-13T16%3A20%3A20Z.png'}]}" }'
  deviceId=EDU-B90F433E-f4-0f-24-19-fe-88
  subFolder=
  deviceNumId=2800007269922577

# JSON string command reply
data=b'{"messageType": "CommandReply", "var": "status", "values": "{\\"name\\":\\"rob\\"}"}'
  deviceId=EDU-B90F433E-f4-0f-24-19-fe-88
  subFolder=
  deviceNumId=2800007269922577
"""


#------------------------------------------------------------------------------
# https://google-cloud-python.readthedocs.io/en/stable/storage/buckets.html
# Copy a file from one storage bucket to another.
# Then delete the file from the source bucket.
# Returns the public URL in the new location, or None for error.
def moveFileBetweenBucketsInCloudStorage(CS, src_bucket, dest_bucket, file_name):
    try:
        src = CS.get_bucket(src_bucket)
        dest = CS.get_bucket(dest_bucket)

        # get image in source bucket
        src_image = src.get_blob(file_name)
        if src_image is None:
            logging.error('moveFileBetweenBucketsInCloudStorage file {} ' \
                    'not found in bucket {}'.format(file_name, src_bucket))
            return None
    
        # copy image to dest bucket
        dest_image = src.copy_blob(src_image, dest)
        dest_image.make_public() # bucket is already public, just for safety 

        # delete the src image
        src_image.delete() # throws an exception, but still works. WTF?
    except:
        pass

    # return the new public url
    return dest_image.public_url


#------------------------------------------------------------------------------
# Save the image bytes to a file in cloud storage.
# The cloud storage bucket we are using allows "allUsers" to read files.
# Return the public URL to the file in a cloud storage bucket.
def saveFileInCloudStorage( CS, varName, imageType, imageBytes, 
        deviceId, CS_BUCKET ):

    bucket = CS.bucket( CS_BUCKET )
    filename = '{}_{}_{}.{}'.format( deviceId, varName,
        time.strftime( '%FT%XZ', time.gmtime() ), imageType )
    blob = bucket.blob( filename )

    content_type = 'image/{}'.format( imageType )

    blob.upload_from_string( imageBytes, content_type=content_type )
    logging.info( "saveFileInCloudStorage: image saved to %s" % \
            blob.public_url )
    return blob.public_url


#------------------------------------------------------------------------------
# Save the URL to an image in cloud storage, as an entity in the datastore, 
# so the UI can fetch it for display / time lapse.
def saveImageURLtoDatastore(DS, deviceId, publicURL, cameraName):
    key = DS.key(DS_images_KEY)
    image = datastore.Entity(key, exclude_from_indexes=[])
    cd = time.strftime( '%FT%XZ', time.gmtime())
    # Don't use a dict, the strings will be assumed to be "blob" and will be
    # shown as base64 in the console.
    # Use the Entity like a dict to get proper strings.
    image['device_uuid'] = deviceId
    image['URL'] = publicURL
    image['camera_name'] = cameraName
    image['creation_date'] = cd
    DS.put(image)  
    logging.info("saveImageURLtoDatastore: saved {}".format( image ))
    return 


#------------------------------------------------------------------------------
# Save a partial b64 chunk of an image to a cache in the datastore.
#TODO: deprecated after September 30, 2019 (or there abouts).
def saveImageChunkToDatastore( DS, deviceId, messageId, varName, imageType, \
        chunkNum, totalChunks, imageChunk ):
    key = DS.key( 'MqttServiceCache' )
    # string properties are limited to 1500 bytes if indexed, 
    # 1M if not indexed.  
    chunk = datastore.Entity( key, exclude_from_indexes=['imageChunk'] )
    chunk.update( {
        'deviceId': deviceId,
        'messageId': messageId,
        'varName': varName,
        'imageType': imageType,
        'chunkNum': chunkNum,
        'totalChunks': totalChunks,
        'imageChunk': imageChunk,
        'timestamp': datetime.now()
        } )
    DS.put( chunk )  
    logging.debug( 'saveImageChunkToDatastore: saved to MqttServiceCache '
        '{}, {} of {} for {}'.format( 
            messageId, chunkNum, totalChunks, deviceId ))
    return 


#------------------------------------------------------------------------------
# Returns list of dicts, each with a chunk.
#TODO: deprecated after September 30, 2019 (or there abouts).
def getImageChunksFromDatastore( DS, deviceId, messageId ):
    query = DS.query( kind='MqttServiceCache' )
    query.add_filter( 'deviceId', '=', deviceId )
    query.add_filter( 'messageId', '=', messageId )
    qiter = list( query.fetch() )
    results = list( qiter )
    resultsToReturn = []
    for row in results:
        pydict = {
            'deviceId': row.get( 'deviceId', '' ),
            'messageId': row.get( 'messageId', '' ),
            'varName': row.get( 'varName', '' ),
            'imageType': row.get( 'imageType', '' ),
            'chunkNum': row.get( 'chunkNum', '' ),
            'totalChunks': row.get( 'totalChunks', '' ),
            'imageChunk': row.get( 'imageChunk', '' ) 
        }
        resultsToReturn.append( pydict )
    return resultsToReturn


#------------------------------------------------------------------------------
#TODO: deprecated after September 30, 2019 (or there abouts).
def deleteImageChunksFromDatastore( DS, deviceId, messageId ):
    query = DS.query( kind='MqttServiceCache' )
    query.add_filter( 'deviceId', '=', deviceId )
    query.add_filter( 'messageId', '=', messageId )
    qiter = query.fetch()
    for entity in qiter:
        DS.delete( entity.key )
        logging.debug( "deleteImageChunksFromDatastore: chunk {} of messageId {} deleted.".format( entity.get( 'chunkNum', '?' ), messageId ))
    return


#------------------------------------------------------------------------------
# Save the ids of an invalid image, so we can clean up the cache.
#TODO: deprecated after September 30, 2019 (or there abouts).
def saveTurd( DS, deviceId, messageId ):
    key = DS.key( 'MqttServiceTurds' )
    turd = datastore.Entity( key )
    turd.update( {
        'deviceId': deviceId,
        'messageId': messageId,
        'timestamp': datetime.now()
        } )
    DS.put( turd )  
    logging.debug( 'saveTurd: saved to MqttServiceTurds {} for {}'.format( 
            messageId, deviceId ))
    return 


#------------------------------------------------------------------------------
# Returns list of dicts, each with a chunk.
#TODO: deprecated after September 30, 2019 (or there abouts).
def getTurds( DS, deviceId ):
    query = DS.query( kind='MqttServiceTurds' )
    query.add_filter( 'deviceId', '=', deviceId )
    qiter = list( query.fetch() )
    results = list( qiter )
    resultsToReturn = []
    for row in results:
        pydict = {
            'deviceId': row.get( 'deviceId', '' ),
            'messageId': row.get( 'messageId', '' )
        }
        resultsToReturn.append( pydict )
    return resultsToReturn


#------------------------------------------------------------------------------
#TODO: deprecated after September 30, 2019 (or there abouts).
def deleteTurd( DS, deviceId, messageId ):
    query = DS.query( kind='MqttServiceTurds' )
    query.add_filter( 'deviceId', '=', deviceId )
    query.add_filter( 'messageId', '=', messageId )
    qiter = query.fetch()
    for entity in qiter:
        DS.delete( entity.key )
    logging.debug( "deleteTurd: messageId {} deleted.".format( messageId ))
    return


# ------------------------------------------------------------------------------
# Check if the file is in the uploads bucket yet (can take a bit for file to
# show up in the bucket).
# Returns True or False.
def isUploadedImageInBucket(CS, file_name, src_bucket_name):

    src_bucket = CS.get_bucket(src_bucket_name)
    src_image = src_bucket.get_blob(file_name)
    if src_image is None:
        logging.debug("isUploadedImageInBucket: file NOT in bucket={}"
                .format(file_name))
        return False # image not in bucket (yet)

    logging.debug("isUploadedImageInBucket: file={} in bucket={}"
            .format(file_name, src_bucket_name))
    return True # image is here


#------------------------------------------------------------------------------
# New way of handling images.  
# The image has already been uploaded to a GCP bucket via a public
# firebase cloud function.   (in an open and un-secured manner) 
# This is just a message telling us it was done (over the secure IoT 
# messaging) and gives us a hook to move the image and save its URL into DS, BQ.
def save_uploaded_image(CS, DS, BQ, pydict, deviceId, \
                PROJECT, DATASET, TABLE, CS_BUCKET):
    try:
        if messageType_ImageUpload != validateMessageType( pydict ):
            logging.error("save_uploaded_image: invalid message type")
            return

        # each received image message must have these fields
        if not validDictKey( pydict, varName_KEY ) or \
           not validDictKey( pydict, fileName_KEY ):
            logging.error('save_uploaded_image: missing key(s) in dict.')
            return

        var_name =  pydict[ varName_KEY ]
        file_name = pydict[ fileName_KEY ]

        start = datetime.now()
        # get a timedelta of the difference
        delta = datetime.now() - start

        # keep checking for image curl upload for 5 minutes
        while delta.total_seconds() <= 5 * 60:

            # Has this image already been handled?
            # (this can happen since google pub-sub is "at least once" message
            # delivery, the same message can get delivered again)
            if isUploadedImageInBucket(CS, file_name, CS_BUCKET):
                logging.info(f'save_uploaded_image: file {file_name} already handled.')
                break

            # Check if the file is in the upload bucket.
            if not isUploadedImageInBucket(CS, file_name, UPLOAD_BUCKET_NAME):
                time.sleep(10)
                delta = datetime.now() - start
                logging.debug(f"save_uploaded_image: waited {delta.total_seconds()} secs for upload of {file_name}")
                continue

            # Move image from one gstorage bucket to another:
            #   openag-public-image-uploads > openag-v1-images
            publicURL = moveFileBetweenBucketsInCloudStorage(CS, \
                    UPLOAD_BUCKET_NAME, CS_BUCKET, file_name)
            if publicURL is None:
                logging.warning(f'save_uploaded_image: image already moved: {file_name}')
                break

            # Put the URL in the datastore for the UI to use.
            saveImageURLtoDatastore(DS, deviceId, publicURL, var_name)

            # Put the URL as an env. var in BQ.
            message_obj = {}
            # keep old message type, UI code may depend on it
            message_obj[ messageType_KEY ] = messageType_Image
            message_obj[ var_KEY ] = var_name
            valuesJson = "{'values':["
            valuesJson += "{'name':'URL', 'type':'str', 'value':'%s'}" % \
                    (publicURL)
            valuesJson += "]}"
            message_obj[ values_KEY ] = valuesJson
            bq_data_insert(BQ, message_obj, deviceId, \
                    PROJECT, DATASET, TABLE )

            delta = datetime.now() - start
            logging.info(f"save_uploaded_image: Done with {file_name} in {delta.total_seconds()} secs")
            break


        # Remove any files in the uploads bucket that are over 2 hours old
        now = datetime.now(timezone.utc) # use same TZ as storage
        bucket = CS.get_bucket(UPLOAD_BUCKET_NAME)
        blobs = bucket.list_blobs()
        for blob in blobs:
            time_created = blob.time_created # datetime or None
            delta = now - time_created
            if delta.total_seconds() >= 2 * 60 * 60:
                try:
                    blob.delete()
                except:
                    pass 
                logging.info(f'save_uploaded_image: Removing stale file={blob.path}')

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical(f"Exception in save_uploaded_image(): {e}")
        traceback.print_tb( exc_traceback, file=sys.stdout )


#------------------------------------------------------------------------------
# Parse and save the image.
#TODO, deprecated code below, remove when all devices are updated to latest code as of April 30, 2019.
def save_image( CS, DS, BQ, pydict, deviceId, PROJECT, DATASET, TABLE, \
        CS_BUCKET ):
    try:
        if messageType_Image != validateMessageType( pydict ):
            logging.error( "save_image: invalid message type" )
            return

        # each received image message must have these fields
        if not validDictKey( pydict, varName_KEY ) or \
                not validDictKey( pydict, imageType_KEY ) or \
                not validDictKey( pydict, chunk_KEY ) or \
                not validDictKey( pydict, totalChunks_KEY ) or \
                not validDictKey( pydict, imageChunk_KEY ) or \
                not validDictKey( pydict, messageID_KEY ):
            logging.error('save_image: Missing key(s) in dict.')
            return

        messageId =   pydict[ messageID_KEY ]
        varName =     pydict[ varName_KEY ]
        imageType =   pydict[ imageType_KEY ]
        chunkNum =    pydict[ chunk_KEY ]
        totalChunks = pydict[ totalChunks_KEY ]
        imageChunk =  pydict[ imageChunk_KEY ]

        # Get rid of all chunks if we receive one bad chunk - so we don't 
        # make bad partial images.
        if 0 == len(imageChunk):
            logging.error( "save_image: received empty imageChunk from {}, cleaning up turds".format( deviceId ))
            deleteImageChunksFromDatastore( DS, deviceId, messageId )
            saveTurd( DS, deviceId, messageId )
            return

        # Clean up any smelly old turds from previous images (if they don't
        # match the current messageId from this device).
        turds = getTurds( DS, deviceId )
        for badImage in turds:
            badMessageId = badImage['messageId'] 
            if badMessageId != messageId:
                deleteImageChunksFromDatastore( DS, deviceId, badMessageId )
                deleteTurd( DS, deviceId, badMessageId )

        # Save this chunk to the datastore cache.
        saveImageChunkToDatastore( DS, deviceId, messageId, varName, 
            imageType, chunkNum, totalChunks, imageChunk )

        # For every message received, check data store to see if we can
        # assemble chunks.  Messages will probably be received out of order.

        # Start with a list of the number of chunks received:
        listOfChunksReceived = []
        for c in range( 0, totalChunks ):
            listOfChunksReceived.append( False )

        # What chunks have we already received? 
        oldChunks = getImageChunksFromDatastore( DS, deviceId, messageId )
        for oc in oldChunks:
            listOfChunksReceived[ oc[ 'chunkNum' ] ] = True
            logging.debug( 'save_image: received {} of {} '
                'for messageId={}'.format( oc[ 'chunkNum'], 
                    totalChunks, messageId))

        # Do we have all chunks?
        haveAllChunks = True
        chunkCount = 0 
        for c in listOfChunksReceived:
            logging.debug( 'save_image: listOfChunksReceived [{}]={}'.format(
                chunkCount, c))
            chunkCount += 1 
            if not c:
                haveAllChunks = False
        logging.debug( 'save_image: haveAllChunks={}'.format(haveAllChunks))

        # No, so just add this chunk to the datastore and return
        if not haveAllChunks:
            logging.debug('save_image: returning to wait for more chunks')
            return

        # YES! We have all our chunks, so reassemble the binary image.

        # Delete the temporary datastore cache for the chunks
        deleteImageChunksFromDatastore( DS, deviceId, messageId )
        deleteTurd( DS, deviceId, messageId )

        # Sort the chunks by chunkNum (we get messages out of order)
        oldChunks = sorted( oldChunks, key=lambda k: k['chunkNum'] )

        # Reassemble the b64 chunks into one string (in order).
        b64str = ''
        for oc in oldChunks:
            b64str += oc[ 'imageChunk' ]
            logging.debug( 'save_image: assemble {} of {}'.format( 
                oc[ 'chunkNum' ], oc['totalChunks'] ))
            
        # Now covert our base64 string into binary image bytes
        imageBytes = base64.b64decode( b64str )

        # Put the image bytes in cloud storage as a file, and get an URL
        publicURL = saveFileInCloudStorage( CS, varName, imageType,
            imageBytes, deviceId, CS_BUCKET )
        
        # Put the URL in the datastore for the UI to use.
        saveImageURLtoDatastore( DS, deviceId, publicURL, varName )

        # Put the URL as an env. var in BQ.
        message_obj = {}
        message_obj[ messageType_KEY ] = messageType_Image
        message_obj[ var_KEY ] = varName
        valuesJson = "{'values':["
        valuesJson += "{'name':'URL', 'type':'str', 'value':'%s'}" % \
                            ( publicURL )
        valuesJson += "]}"
        message_obj[ values_KEY ] = valuesJson
        bq_data_insert( BQ, message_obj, deviceId, PROJECT, DATASET, TABLE )

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical( "Exception in save_image(): %s" % e)
        traceback.print_tb( exc_traceback, file=sys.stdout )


#------------------------------------------------------------------------------
# Internal method to get the value from a string of data from the device
# or DB.  Handles weird stuff like a string in a string.
def _string_to_value( string ):
    try:
        values = ast.literal_eval( string ) # if this works, great!
        firstVal = values['values'][0]
        return firstVal['value']
    except:
        # If the above has issues, the string probably has an embedded string.
        # Such as this:
        # "{'values':[{'name':'LEDPanel-Top', 'type':'str', 'value':'{'400-449': 0.0, '450-499': 0.0, '500-549': 83.33, '550-559': 16.67, '600-649': 0.0, '650-699': 0.0}'}]}"
        valueTag = "\'value\':\'"
        endTag = "}]}"
        valueStart = string.find( valueTag )
        valueEnd = string.find( endTag )
        if -1 == valueStart or -1 == valueEnd:
            return string
        valueStart += len( valueTag )
        valueEnd -= 1
        val = string[ valueStart:valueEnd ]
        return ast.literal_eval( val ) # let exceptions from this flow up
    return string


#------------------------------------------------------------------------------
# Internal method to get the name from a string of data from the device
# or DB.  Handles weird stuff like a string in a string.
def _string_to_name( string ):
    try:
        values = ast.literal_eval( string ) # if this works, great!
        firstVal = values['values'][0]
        return firstVal['name']
    except:
        # If the above has issues, the string probably has an embedded string.
        # Such as this:
        # "{'values':[{'name':'LEDPanel-Top', 'type':'str', 'value':'{'400-449': 0.0, '450-499': 0.0, '500-549': 83.33, '550-559': 16.67, '600-649': 0.0, '650-699': 0.0}'}]}"
        nameTag = "\'name\':\'"
        endTag = "\'"
        nameStart = string.find( nameTag )
        if -1 == nameStart:
            return None
        nameStart += len( nameTag )
        nameEnd = string.find( endTag, nameStart )
        if -1 == nameEnd:
            return None
        name = string[ nameStart:nameEnd ]
        return name
    return ''


#------------------------------------------------------------------------------
# Save a bounded list of the recent values of each env. var. to the Device
# that produced them - for UI display / charting.
def save_data_to_Device( DS, pydict, deviceId ):
    try:
        if messageType_EnvVar != validateMessageType( pydict ) and \
           messageType_CommandReply != validateMessageType( pydict ):
            return

        # each received EnvVar type message must have these fields
        if not validDictKey( pydict, var_KEY ) or \
            not validDictKey( pydict, values_KEY ):
            logging.error('save_data_to_Device: Missing key(s) in dict.')
            return
        varName = pydict[ var_KEY ]

        value = _string_to_value( pydict[ values_KEY ] )
        name = _string_to_name( pydict[ values_KEY ] )
        valueToSave = { 
                'timestamp': str( time.strftime( '%FT%XZ', time.gmtime())),
                'name': str( name ),
                'value': str( value ) }

        # Get this device data from the datastore (or create an empty one).
        # These DeviceData entities are custom keyed with our deviceId.
        ddkey = DS.key( DS_device_data_KEY, deviceId )
        dd = DS.get( ddkey ) 
        if not dd: 
            # The device data entity doesn't exist, so create it
            dd = datastore.Entity( ddkey )
            dd.update( {} ) # empty entity
            DS.put( dd ) # write to DS

        # retry the Entity update in a transaction until it succeeds
        transactionWorked = False
        for _ in range( 15 ):
            try:
                with DS.transaction():
                    dd = DS.get( ddkey )

                    # get a property named for the env var, which is a list of
                    # dict values
                    valuesList = dd.get( varName, [] )

                    # put this value at the front of the list
                    valuesList.insert( 0, valueToSave )
                    # cap max size of list
                    while len( valuesList ) > DS_env_vars_MAX_size:
                        valuesList.pop() # remove last item in list

                    # update the entity
                    dd[ varName ] = valuesList 

                    # save the entity to the datastore
                    dd.exclude_from_indexes = dd.keys()
                    DS.put( dd )  
                    transactionWorked = True
                    break
            except Exception as e:
                #logging.debug('save_data_to_Device: transaction failed '\
                #        '{}'.format( e ))
                continue
        if not transactionWorked:
            logging.error('save_data_to_Device: transaction failed for ' \
                    'deviceId={} var={}'.format( deviceId, varName ))
            return

        logging.info('save_data_to_Device: deviceId={} varName={} '\
                'valuesToSave={}'.format( deviceId, varName, valueToSave ))

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical( "Exception in save_data_to_Device(): %s" % e)
        traceback.print_tb( exc_traceback, file=sys.stdout )


#------------------------------------------------------------------------------
# This method is the entry point for all data sent to our service, once 
# the message type is validated.
# Parse and save the (json/dict) data to the appropriate place.
def save_data(CS, DS, BQ, pydict, deviceId, \
        PROJECT, DATASET, TABLE, CS_BUCKET):

    #TODO, deprecated code block below, remove when all devices are updated to latest code as of September 30, 2019 (or there abouts).
    if messageType_Image == validateMessageType( pydict ):
        save_image(CS, DS, BQ, pydict, deviceId, PROJECT, DATASET, TABLE, 
                CS_BUCKET)
        return 

    # New way of handling (already) uploaded images.  
    if messageType_ImageUpload == validateMessageType(pydict):
        save_uploaded_image(CS, DS, BQ, pydict, deviceId, \
                PROJECT, DATASET, TABLE, CS_BUCKET)
        return;

    # Save the most recent data as properties on the Device entity in the
    # datastore.
    save_data_to_Device( DS, pydict, deviceId )

    # Also insert into BQ (Env vars and command replies)
    bq_data_insert( BQ, pydict, deviceId, PROJECT, DATASET, TABLE )


#------------------------------------------------------------------------------
# Insert data into our bigquery dataset and table.
def bq_data_insert( BQ, pydict, deviceId, PROJECT, DATASET, TABLE ):
    try:
        # Generate the data that will be sent to BigQuery for insertion.
        # Each value must be a row that matches the table schema.
        rowList = []
        if not makeBQRowList( pydict, deviceId, rowList ):
            return False
        logging.info( "bq insert rows: {}".format( rowList ))

        dataset_ref = BQ.dataset( DATASET, project=PROJECT )
        table_ref = dataset_ref.table( TABLE )
        table = BQ.get_table( table_ref )               

        response = BQ.insert_rows( table, rowList )
        logging.debug( 'bq response: {}'.format( response ))

#TODO: need to look up the the User in the Datastore by deviceId, and find their openag flag (or role), to know the correct DATASET to write to.

        return True

    except Exception as e:
        logging.critical( "bq_data_insert: Exception: %s" % e )
        return False




