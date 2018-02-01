# A comprehensive Twitter-Kafka-Spark Streamer

## Running this code is not spotless. This is a quickstart
Run spark using 
```
docker compose up
```
Run zookeeper using one from Confluent Platform
```
docker run -d \
    --net=host \
    --name=zookeeper \
    -e ZOOKEEPER_CLIENT_PORT=32181 \
    confluentinc/cp-zookeeper:latest
```    
Run Kafka broker using one from confluent Platform again
```
docker run -d \
       --net=host \
       --name=kafka \
       -e KAFKA_ZOOKEEPER_CONNECT=localhost:32181 \
       -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092 \
       -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
       confluentinc/cp-kafka:4.0.0```
```
Create a topic 'twitterstream' like so
```
docker run \
  --net=host \
  --rm confluentinc/cp-kafka:latest \
  kafka-topics --create --topic twitterstream --partitions 1 --replication-factor 1 --if-not-exists --zookeeper localhost:32181
```
Can check it using (optional)
```
docker run \
  --net=host \
  --rm \
  confluentinc/cp-kafka:latest \
  kafka-topics --describe --topic twitterstream --zookeeper localhost:32181
```  

twitter-streaming contains code that calls Twitter API and produces to kafka. For that to function, install 
```$xslt
pip install tweepy
pip install kafka-python
pip install python-twitter
```
Run as
```$xslt
python twitter-kafkaprod.py <space separated string of filters to put on streams"
```

The other 2 are python codes, they can be ryn using either 
```$xslt
sbt
compile
run
```
Have also added functionality for uber jar using
```
sbt assembly
```
The jar thus creater in target/ can be copied and used with spark-submit command.