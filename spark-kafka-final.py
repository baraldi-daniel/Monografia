#Importing libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

#Starting job
if __name__ == "__main__":

    #Setting SparkSession
    spark = SparkSession \
        .builder \
        .appName("Streaming Monografia") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    #Setting schema
    schema = StructType([
        StructField("chave", StringType()),
        StructField("valorA", StringType()),
        StructField("valorB", StringType())
    ])

    #Setting Kafka source
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.126.128:9092") \
        .option("subscribe", "mensagens-produtor") \
        .option("startingOffsets", "earliest") \
        .load()


    #Setting transformation process and dataframes
    value_select_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    value_select_grouped_df = value_select_df.select("value.chave", "value.valorA","value.valorB").groupBy('valorA').agg({"valorB":"sum"}).withColumnRenamed("sum(valorB)", "valorB")
    kafka_target_df = value_select_grouped_df.selectExpr("'chave' as key",
                                                """to_json(named_struct(
                                                'valorA', valorA,
                                                'valorB', valorB )) as value
                                                """)

    #Setting target and writing
    value_writestream_query = kafka_target_df \
       .writeStream \
       .queryName("Query Write Target") \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "192.168.126.128:9092") \
       .option("topic", "mensagens-consumidor") \
       .outputMode("update") \
       .option("checkpointLocation", r"C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\spark\data") \
       .start()

    #Setting to wait completion
    value_writestream_query.awaitTermination()

#Command to submit
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 "C:\Users\baral\OneDrive\Documents\Engenharia de Dados - USP\Monografia\spark\spark-kafka-final.py" 