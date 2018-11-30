
1- Deploy the Spark Cluster:
```
docker stack deploy --compose-file=docker-compose-hdfs.yml spark
```

2 - Increase the number of Spark Workers to 2:
```
docker service scale spark_worker=2
```
3- Set CONTAINER_ID
     
     export CONTAINER_ID=$(docker ps --filter name=master --format "{{.ID}}")

4- Copy required required file to docker

    docker cp airbnb_avg_price.py $CONTAINER_ID:/tmp
    docker cp MontrealAirBnB.csv $CONTAINER_ID:/tmp

5- execute the code

    docker exec $CONTAINER_ID \
    bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    /tmp/airbnb_avg_price.py
```

* Explore the Applications on http://localhost:8080/


