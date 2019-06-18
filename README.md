# mqtt-service
GCloud hosted app engine service that receives published MQTT messages from our devices.

v5.0 of our MQTT service.  Moved to this new repo from the previous version which is in the old (and somewhat messy) openag-cloud-v1 repo.

This project uses our new cloud_common repo as a git submodule.  It is common to all our cloud services.

* One time setup to run locally:
```
./scripts/local_development_one_time_setup.sh
```

* To run this app locally (on OSX/Linux) for testing:
```
./scripts/run_locally.sh
```

* To deploy this app:
```
./scripts/gcloud_deploy.sh
```
