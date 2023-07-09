from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
import pyspark.sql.functions as F

# Realiza una consulta Utilizando la API SQL de Spark
def clientes_pais(df):
    df.createOrReplaceTempView("vResultado")
    clientes = spark.sql("""SELECT nombre, apellido, pais FROM vResultado""")
    return clientes


if __name__ == "__main__":

    # Creación de la sesión Spark
    spark = SparkSession\
        .builder \
        .appName("KafkaIntegration")\
        .master("local[3]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .getOrCreate()

    # Lectura del flujo de datos en streaming desde Kafka
    tiposStreamingDF = (spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka1p:9092, kafka2p:9092, kafka3p:9092")\
        .option("subscribe", "DataTopic")\
        .option("startingOffsets", "earliest")  # desde el principio
        .load())

    
    # Definición del esquema de los datos esperados
    esquema = StructType([
        StructField("id", IntegerType()),
        StructField("nombre", StringType()),
        StructField("apellido", StringType()),
        StructField("edad", IntegerType()),
        StructField("email", StringType()),
        StructField("ubicacion", MapType(StringType(), StringType(), True))
    ])

    # Transformaciones y procesamiento de los datos en streaming
    parsedDF = tiposStreamingDF\
        .select("value")\
        .withColumn("value", F.col("value").cast(StringType()))\
        .withColumn("input", F.from_json(F.col("value"), esquema))\
        .withColumn("id", F.col("input.id"))\
        .withColumn("nombre", F.col("input.nombre"))\
        .withColumn("apellido", F.col("input.apellido"))\
        .withColumn("edad", F.col("input.edad"))\
        .withColumn("email", F.col("input.email"))\
        .withColumn("direccion", F.col("input.ubicacion.direccion"))\
        .withColumn("ciudad", F.col("input.ubicacion.ciudad"))\
        .withColumn("pais", F.col("input.ubicacion.pais"))

    # Cálculo de promedios en streaming
    clientes_paisesDF = clientes_pais(parsedDF)

    # Agrupación de los promedios por país
    groupByClientesPaisesDf = clientes_paisesDF.groupBy("pais").count()

    # Configuración de la salida de datos en streaming
    salida = groupByClientesPaisesDf\
        .writeStream\
        .queryName("query_clientes_paises_kafka")\
        .outputMode("complete")\
        .format("memory")\
        .start()

    """
    Opciones de outputMode:
    - append = memory   # seleccionar, filtrar, selectExpr
    - complete = memory # groupBy avg
    """

    from time import sleep
    for x in range(50):
        # Consulta y muestra los resultados en la consola
        spark.sql("select * from query_clientes_paises_kafka").show(1000, False)
        sleep(1)

    print('llego hasta aqui')

    # Espera hasta que el proceso de escritura en streaming termine
    salida.awaitTermination()