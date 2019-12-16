from pyspark.sql import SparkSession
import pyspark.sql.types as typ
import pyspark.sql.functions as fn

if __name__ == '__main__':
    spark = SparkSession.builder.\
                config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4')\
                .getOrCreate()

    df_raw = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "appointments") \
      .load()

    schema = typ.StructType([
                   typ.StructField('Data',typ.StructType([
                       typ.StructField('AppointmentId',typ.StringType(),True),
                       typ.StructField('Discipline',typ.ArrayType(typ.StringType(),True),True),
                       typ.StructField('TimestampUtc',typ.TimestampType(),True)]),True),
                   typ.StructField('Type',typ.StringType(),True)
              ])

    df = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    df_schema =  df.select(fn.from_json(df.value, schema).alias("event"))

    df_final = df_schema.select("event.Data.AppointmentId","event.Data.Discipline",
                                "event.Data.TimestampUtc","event.Type")
    			
	query = df_final.writeStream.format("json")\
	.option("checkpointLocation", "/home/hadoop/checkpoint/")\
	.option("path", "/home/hadoop/results").start()

    query.awaitTermination()
