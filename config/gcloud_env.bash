# All the Google cloud platform env. vars. we use in our scripts.
# Meant to be sourced in our bash scripts.

# Get the path to parent directory of this file.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $DIR # Go to the project top level dir.

#------------------------------------------------------------------------------
export GCLOUD_PROJECT=openag-v1
export GCLOUD_REGION=us-east1
export GCLOUD_ZONE=us-east1-b
export GOOGLE_APPLICATION_CREDENTIALS=$DIR/config/service_account.json
export FIREBASE_SERVICE_ACCOUNT=$DIR/config/fb_service_account.json

# PubSub topic and subscription that internal notifications are sent to.
export GCLOUD_NOTIFICATIONS_TOPIC_SUBS=notifications

# PubSub topic and subscription that MQTT telementry 'events' are sent to.
export GCLOUD_DEV_EVENTS=device-events

# IoT device registry
export GCLOUD_DEV_REG=device-registry



