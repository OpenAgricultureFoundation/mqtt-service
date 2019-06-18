#!/bin/bash

# Get the path to parent directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $DIR # Go to the project top level dir.

# Deactivate any current python virtual environment we may be running.
if ! [ -z "${VIRTUAL_ENV}" ] ; then
    echo "deactivate"
fi

# All DEPLOYED env vars live in app.yaml for the gcloud GAE deployed app.
# Since we are running locally, we have to set our env. vars.
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  # gcloud_env.bash has not been sourced, so do it now
  source $DIR/config/gcloud_env.bash
fi

# PubSub topic and subscription that MQTT telementry 'events' are sent to.
# This is a special test-only subscription that a test client writes to.
# ONLY used for debugging a locally running service with one client.
export GCLOUD_DEV_EVENTS="device-test"

# BigQuery TEST dataset we write to.
export BQ_DATASET="test"

# Save images to this TEST bucket.
export CS_BUCKET="openag-v1-test-images"

# Has the user setup the local python environment we need?
if ! [ -d pyenv ]; then
  echo 'ERROR: you have not run ./scripts/local_development_one_time_setup.sh'
  exit 1
fi

# Yes, so activate it for this bash process
source pyenv/bin/activate

# Add the top leve dir to the py path so we can pick up the submodule.
export PYTHONPATH=$DIR

# Run our entry point:
python3.6 $DIR/src/mqtt_service.py --log debug

