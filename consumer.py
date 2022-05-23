from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        .option("subscribe", "booking")
        .option("includeHeaders", "true")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .writeStream.option("checkpointLocation", "/tmp/checkpoints")
        .format("parquet")
        .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/data_booking")
        .start()
        .awaitTermination()
    )
