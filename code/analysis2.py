from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F

def processKafkaMessage(df):
    df.createOrReplaceTempView("vResultado")
    processedData = spark.sql("SELECT pais FROM vResultado")
    return processedData

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("KafkaIntegration")\
        .master("local[3]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .getOrCreate()


    tiposStreamingDF = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka1-p:9092, kafka2-p:9092, kafka3-p:9092")\
        .option("subscribe", "DataTopic")\
        .option("startingOffsets", "earliest")\
        .load()
        
    schema = StructType([
        StructField("id", StringType()),
        StructField("nombre", StringType()),
        StructField("apellido", StringType()),
        StructField("edad", IntegerType()),
        StructField("email", StringType()),
        StructField("telefono", StringType()),
        StructField("ubicacion",
                StructType([
                        StructField("direccion", StringType()),
                        StructField("ciudad", StringType()),
                        StructField("pais", StringType())
                    ])
                   ),
        StructField("productovisto", StringType()),
        StructField("taps", FloatType())
    ])
    
    parsedDF = tiposStreamingDF\
        .select(F.col("value").cast(StringType()).alias("value"))\
        .withColumn("input", F.from_json("value", schema))\
        .select("input.*", "input.ubicacion.*")\
        .drop("ubicacion")
        

    processedDataDF = processKafkaMessage(parsedDF)

    clientesPaisesDf = processedDataDF.groupBy("pais").count()

    outputQuery = clientesPaisesDf.writeStream\
        .queryName("query_usuarios_paises_kafka")\
        .outputMode("complete")\
        .format("memory")\
        .start() #.outputMode("update") .outputMode("append")
    
    from time import sleep
    for _ in range(50):
        # Consulta y muestra los resultados en la consola
        spark.sql("select * from query_usuarios_paises_kafka").show(1000, False)
        sleep(1)

    print('llego hasta aqui')

    outputQuery.awaitTermination()
