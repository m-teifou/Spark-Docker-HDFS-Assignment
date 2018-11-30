# Step 08: Spark Cluster

/!\ Check that enough resources (especially Memory / RAM) are allocated to Docker since our Spark Cluster needs the following resources per Worker:
```
SPARK_WORKER_CORES: 2
SPARK_WORKER_MEMORY: 1g
```

- For Mac OS, see https://docs.docker.com/docker-for-mac/#advanced
- For Windows, see https://docs.docker.com/docker-for-windows/#advanced

## Docker Compose

Follow the instructions of [Running Spark with Docker Swarm on DigitalOcean](https://testdriven.io/running-spark-with-docker-swarm-on-digitalocean), from *Project Setup* to *Docker Swarm* (excluded).

To do so, you should use the following Git Repo: https://classroom.github.com/a/yr2PFA8f (which replaces the "Clone down the project repo" initial section).

Take note that the `docker-compose up -d` command makes use _by default_ of the [docker-compose.yml](https://github.com/CCE-BigData/spark-docker-swarm/blob/master/docker-compose.yml) file (you should have a look at this file).

Then, to delete the Spark Cluster: `docker-compose down`

## Docker Swarm

### Spark Cluster
* Test that Docker Swarm is running: `docker service ls`

If required: `docker swarm init`

* Deploy the Spark Cluster:
```
docker stack deploy --compose-file=docker-compose-hdfs.yml spark
```

* Increase the number of Spark Workers to 2:
```
docker service scale spark_worker=2
```

* List the deployed services: `docker service ls`

Check http://localhost:8080/ to see that there is indeed now 2 Registered Spark Workers.

* References:
  - https://docs.docker.com/swarm/
  - https://thenewstack.io/kubernetes-vs-docker-swarm-whats-the-difference/

### PySpark Deployment

The PySpark script to deploy: [count.py](https://github.com/CCE-BigData/spark-docker-swarm/blob/master/count.py)

```
export CONTAINER_ID=$(docker ps --filter name=master --format "{{.ID}}")

docker cp count-rdd.py $CONTAINER_ID:/tmp

docker exec $CONTAINER_ID \
  bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    /tmp/count.py
```

* Explore the Applications on http://localhost:8080/

### Interactive mode

In interactive mode (`docker exec -it`), compute how many lines of the "README.md" file do contain the "Spark" word.
Reference: https://spark.apache.org/docs/2.3.0/quick-start.html#interactive-analysis-with-the-spark-shell

### HDFS

#### Spark RDD

The PySpark script to deploy: [count-rdd.py](https://github.com/CCE-BigData/spark-docker-swarm/blob/master/count-rdd.py)

```
export CONTAINER_ID=$(docker ps --filter name=master --format "{{.ID}}")

docker cp count-rdd.py $CONTAINER_ID:/tmp

docker exec $CONTAINER_ID \
  bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    /tmp/count-rdd.py
```

References:
- https://hub.docker.com/r/harisekhon/hadoop/
- https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-common/FileSystemShell.html

To browse the files on HDFS:
http://localhost:50070 -> "Utilities/Browse the file system"

To have a look at the content of the saved files on HDFS:
`docker exec -it $(docker ps --filter name=master --format "{{.ID}}") hdfs dfs -cat hdfs://hadoop:8020/user/me/count-rdd/part-00000*`

#### Spark DataFrame

Do the same with the [count-dataframe.py](https://github.com/CCE-BigData/spark-docker-swarm/blob/master/count-dataframe.py) script.

* Why using `DataFrame` instead of `RDD`? You should have a look at both [count-rdd.py](https://github.com/CCE-BigData/spark-docker-swarm/blob/master/count-rdd.py) & [count-dataframe.py](https://github.com/CCE-BigData/spark-docker-swarm/blob/master/count-dataframe.py) to get some answers.

#### Epilogue

Delete the current Spark Services.

To know how to do so, explore the Docker Swarm commands: `docker stack --help`

# Individual Support of the Individual Assignment

You will have also to deploy your code into a Spark + HDFS Cluster
