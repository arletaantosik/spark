# install library in databricks
# library source: maven; coordinates: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 

BOOTSTRAP_SERVER = "xyz:9092"
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule" #connecting from spark to kafka cluster, spark runs on JVM; security protocol, username, password handled by jaas_module in JVM
CLUSTER_API_KEY = "xyz"
CLUSTER_API_SECRET = "xyz"

df = ( spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", f"{JAAS_MODULE} required username='{CLUSTER_API_KEY}' password='{CLUSTER_API_SECRET}';")
            .option("subscribe", "invoices")
            .load()
        )


display(df)
# to see data from kafka; binary bytecode
# timpestamp - when data was created by the producer, timestampType - when the message was received at the kafka cluster
# if timestampType=0, then timestamp is event time, when the data was sent by producer
