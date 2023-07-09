# Se importan las librerías necesarias

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F

# Función que obtiene el nombre, telefono y correo del cliente, así como el producto visto ál menos 30 segundos
def clientes_notificar(df):
    df.createOrReplaceTempView("vProductosVistos")

    # Realizar la consulta SQL
    result_df = spark.sql("""
                            SELECT id, nombre, telefono, email, productovisto 
                            FROM vProductosVistos  
                            WHERE taps >= 30.0
                        """)
    return result_df

def enviar_notificacion(nombre, productovisto): # Simula el envío de notificación por SMS, App o Chat
     
    mensaje = f"\n\n\n\n\n\nEstimad@ {nombre},\n\n¡Esperamos que estés bien! Queríamos recordarte que has visto el producto '{productovisto}' en nuestra tienda en línea. ¡No pierdas la oportunidad de adquirirlo!\n\nSi tienes alguna pregunta o necesitas más información, no dudes en contactarnos. ¡Estamos aquí para ayudarte!\n\n¡Gracias y que tengas un excelente día!\n\nAtentamente,\nTu equipo de ventas\n\n\n\n\n\n"
    # Lógica para enviar la notificación
    print(mensaje)


if __name__ == "__main__":

    # Se crea una Sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("KafkaIntegration")\
        .master("local[3]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .getOrCreate()

    # Se crea un DataFrame con la información que se recibe de Kafka
    streaming_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "kafka1-p:9092, kafka2-p:9092, kafka3-p:9092")\
        .option("subscribe", "DataTopic")\
        .option("startingOffsets", "earliest")\
        .load()

    # Se crea el eschema de la data recibida    
    schema = StructType([
        StructField("id", StringType()),
        StructField("nombre", StringType()),
        StructField("apellido", StringType()),
        StructField("edad", IntegerType()),
        StructField("email", StringType()),
        StructField("telefono", StringType()),
        StructField("ubicacion", StructType([
            StructField("direccion", StringType()),
            StructField("ciudad", StringType()),
            StructField("pais", StringType())
        ])),
        StructField("productovisto", StringType()),
        StructField("taps", FloatType())
    ])
    
    # Se genera una columna por cada clave
    parsed_df = streaming_df\
        .select(F.col("value").cast(StringType()).alias("value"))\
        .withColumn("input", F.from_json("value", schema))\
        .select("input.*", "input.ubicacion.*")\
        .drop("ubicacion")

    # Se llama la función ''clientes-notificar
    clientes_notificados_df = clientes_notificar(parsed_df)

    # Definir la consulta en modo Append (para notificar solo a los nuevos registros)
    # Se ejecuta la consulta cada 9 segundos
    query = clientes_notificados_df.writeStream\
        .outputMode("append") \
        .trigger(processingTime="9 seconds") \
        .foreach(lambda row: enviar_notificacion(row.nombre, row.productovisto)) \
        .start()
    
    query.awaitTermination()
