from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        .option("subscribe", "booking")
        .option("includeHeaders", "true")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .write.parquet(
            path="hdfs://sandbox-hdp.hortonworks.com:8020/data_booking/batch",
            mode="append",
        )
    )
