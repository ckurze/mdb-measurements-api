# Demo for MongoDB OpsManager API for importing measurements

Example API calls to OpsManager in order to query measurements for all databases, disk partitions, and hosts of a cluster.

In order to query MongoDB Atlas API, slightly different calls are needed.

Please refer to the documentation for further information:
* OpsManager API Resources: https://docs.opsmanager.mongodb.com/current/reference/api/measurements/
* Atlas API Resources: https://docs.atlas.mongodb.com/reference/api/monitoring-and-logs/

## Usage

```
usage: import_metrics.py [-h] --opsmanager_baseurl OPSMANAGER_BASEURL
                         --opsmanager_username OPSMANAGER_USERNAME
                         --opsmanager_apikey OPSMANAGER_APIKEY
                         --opsmanager_groupid OPSMANAGER_GROUPID
                         --opsmanager_clusterid OPSMANAGER_CLUSTERID
                         --target_mongouri TARGET_MONGOURI
                         --target_mongodatabase TARGET_MONGODATABASE
                         --metrics_start METRICS_START --metrics_end
                         METRICS_END --metrics_granularity METRICS_GRANULARITY
```

Example call:
```
python import_metrics.py \
--opsmanager_baseurl="http://ec2-XXX-XXX-XXX-XXX.eu-central-1.compute.amazonaws.com:8080/api/public/v1.0" \
--opsmanager_username="OPSMANAGER_OR_ATLAS_USER" \
--opsmanager_apikey=API_KEY \
--opsmanager_groupid=5d2f2e6dcc6d0c132149b94c \
--opsmanager_clusterid=5d4bc93ccc6d0c1312045ad9 \
--target_mongouri="mongodb://USER:PASSWORD@democluster-shard-00-00-XXXXX.mongodb.net:27017,democluster-shard-00-01-XXXXX.mongodb.net:27017,democluster-shard-00-02-XXXXX.mongodb.net:27017/test?ssl=true&replicaSet=DemoCluster-shard-0&authSource=admin&retryWrites=true&w=majority" \
--target_mongodatabase=metrics_test \
--metrics_start=2019-08-08T07:20:00Z \
--metrics_end=2019-08-08T07:40:00Z \
--metrics_granularity=PT10S
```

## Created Collections

* `available_measurements_database`: all available measures for a database (= output of API call)
* `available_measurements_disk`: all available measures for a disk partition (= output of API call)
* `available_measurements_host`: all available measures for a host (= output of API call)
* `clusters`: detailled information about the cluster (as provided by the id calling the script) (= output of API call)
* `cluster_hosts`: all hosts of the cluster (= output of API call)
* `cluster_host_databases`: all databases on each host of the cluster (= output of API call)
* `cluster_host_disk_partitions`: all disk partitions of each host (= output of API call)
* `cluster_summarized_info`: the summarized information used for querying the measurements
* `metrics_hosts`: the hardware metrics for each host of the provided timespan and granularity (= output of API call)
* `metrics_databases`: the database statistics for each database on each host of the provided timespan and granularity (= output of API call)
* `metrics_disk_partitions`: the disk usage measurements for each disk partition on each host of the provided timespan and granularity (= output of API call)
