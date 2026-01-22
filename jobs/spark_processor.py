from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"

SOURCE_TOPIC = "financial_transactions"
AGGREGATION_TOPIC = 'transaction_aggregations'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR = "/mnt/spark-checkpoints"

spark = (SparkSession.builder
         .appName("FinancialTransactionalProcessor")
         .config("spark.jars.packages", 'org.apache.spark-sql-kafka-0-10_2.12:3.5.1')
         .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
         .config('spark.sql.shuffle.partitions', 20)
         ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
  StructField("transactionId", StringType(), True),
  StructField("userId", StringType(), True),
  StructField("merchantId", StringType(), True),
  StructField("amount", DoubleType(), True),
  StructField("transactionTime", LongType(), True),
  StructField("transactionType", StringType(), True),
  StructField("location", StringType(), True),
  StructField("paymentMethod", StringType(), True),
  StructField("isInternational", StringType(), True),
  StructField("currency", StringType(), True)
])

kafka_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers",KAFKA_BROKERS)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss","false")
                ).load()

transaction_df = kafka_stream.selectExpr("CAST(value AS STRING) AS value") \
                        .select(from_json(col('value'),transaction_schema).alias("data")) \
                        .select("data.*")

transaction_df = transaction_df.withColumn('transactionTimestamp',(col('transactionTime') / 1000).cast("timestamp"))
# transaction_df.printSchema()


aggregated_df = transaction_df.withWatermark("transactionTimestamp", "10 seconds") \
                                .groupBy(
                                    window(col("transactionTimestamp"), "30 seconds"),
                                    col("merchantId")) \
                              .agg(
                                sum("amount").alias("totalAmount"),
                                count("*").alias("transactionalCount")
                              )

# aggregated_df.printSchema()


aggregation_query = (
    aggregated_df
    .withColumn("key", col("merchantId").cast("string"))
    .withColumn(
        "value",to_json(
            struct(
                col("merchantId"),
                col("totalAmount"),
                col("transactionalCount")
            )
        )
    )) \
    .selectExpr("key", "value") \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", AGGREGATION_TOPIC) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates") \
    .start()



# aggregation_query.start()

aggregation_query.awaitTermination()

#docker exec -it the12billionrecords-spark-master-1 /opt/spark/bin/spark-submit  --master spark://spark-master:7077  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2 --conf spark.jars.ivy=/tmp/.ivy2 /opt/spark/jobs/spark_processor.py