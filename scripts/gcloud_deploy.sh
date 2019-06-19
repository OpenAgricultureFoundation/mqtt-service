#!/bin/bash

# Get the path to parent directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $DIR # Go to the project top level dir.

# All DEPLOYED env vars live in app.yaml for the gcloud GAE deployed app.
# Since we are running locally, we have to set our env. vars.
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  # gcloud_env.bash has not been sourced, so do it now
  source $DIR/config/gcloud_env.bash
fi

# Authorize using our service account and local key.
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

# The configuration will be saved to:
#   ~/.config/gcloud/configurations/config_default
gcloud config set project $GCLOUD_PROJECT
gcloud config set compute/region $GCLOUD_REGION
gcloud config set compute/zone $GCLOUD_ZONE
gcloud config list

# To handle the fact that app engine can't deal with git submodules, 
# make a tempporary copy of all the source and deploy that.
rm -fr tmp
mkdir tmp
cp app.yaml LICENSE README.md tmp/
cp -R cloud_common config docs scripts src tmp/
rm tmp/config/*service_account.json

# Now make a combined (service + cloud_common submodule) reqs.
# (app engine can't handle includes)
rm -f tmp/requirements.txt 
touch tmp/requirements.txt 
cat config/requirements.txt >> tmp/requirements.txt 
cat cloud_common/config/requirements.txt >> tmp/requirements.txt 

cd tmp
gcloud app deploy

#gcloud app open-console
#gcloud app instances list
#gcloud app instances ssh --service default --version <ver> <ID>
#gcloud app versions list --hide-no-traffic
#gcloud app versions stop <ver>
#gcloud app instances delete --service default --version <ver> <ID>
