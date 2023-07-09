from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Crear la sesión de Spark
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Definir la estructura del esquema de los datos
schema = "id INT, nombre STRING, apellido STRING, edad INT, correo STRING, telefono STRING, direccion STRING, ciudad STRING, pais STRING, productovisto DOUBLE, taps DOUBLE"

# Leer los datos de Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nombre_del_topic") \
    .load()

# Convertir los datos en formato string
value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parsear los datos en formato JSON y aplicar el esquema definido
parsed_df = value_df.selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, '" + schema + "') AS data") \
    .select("data.*")

# Crear una vista temporal de la tabla
parsed_df.createOrReplaceTempView("datos")

# Realizar la consulta SQL
result_df = spark.sql("SELECT nombre, telefono, correo, productovisto FROM datos WHERE taps > 30.0")

# Definir la función enviar_notificacion
def enviar_notificacion(nombre, productovisto):
    mensaje = f"Nombre: {nombre}, Producto Visto: {productovisto}"
    # Lógica para enviar la notificación
    print(mensaje)

# Definir la consulta de escritura en archivo Parquet
query = result_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "ruta_del_archivo_parquet") \
    .option("checkpointLocation", "ruta_del_checkpoint") \
    .trigger(processingTime="15 seconds") \
    .foreach(lambda row: enviar_notificacion(row.nombre, row.productovisto)) \
    .start()

# Esperar a que la consulta termine
query.awaitTermination()
