# Spark Streaming demonstration with CSV file and Kafka stream input

# Input file used:

    https://drive.google.com/drive/folders/1mDmzNS47-q5ehSJ29kRlWRD-8allSRz_
    split -l 100000 london_crime_by_lsoa.csv

# [Kafka setup](https://kafka.apache.org/quickstart)

## Start Zookeeper
`bin/zookeeper-server-start.sh config/zookeeper.properties`

## Start Kafka broker
`bin/kafka-server-start.sh config/server.properties`

## Create Kafka topic
`bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic lcr-events`

### Eg. console producer
`bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-replicated-topic`
### Eg. console consumer 
`bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic my-replicated-topic`

### Eg. standalone producer and consumer
`bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties`

## Kafka streams test

    ~/works/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 stream_kafka_write.py
    ~/works/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 stream_kafka_read.py

