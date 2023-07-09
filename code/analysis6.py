from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F

def clientes_notificar(df):
    df.createOrReplaceTempView("vProductosVistos")

    result_df = spark.sql("""
                            SELECT id, nombre, telefono, email, productovisto 
                            FROM vProductosVistos  
                            WHERE taps >= 30.0
                        """)
    return result_df

def enviar_notificacion(nombre, productovisto):
    mensaje = f"\n\n\n\n\n\nEstimado {nombre},\n\n¡Esperamos que estés bien! Queríamos recordarte que has visto el producto '{productovisto}' en nuestra tienda en línea. ¡No pierdas la oportunidad de adquirirlo!\n\nSi tienes alguna pregunta o necesitas más información, no dudes en contactarnos. ¡Estamos aquí para ayudarte!\n\n¡Gracias y que tengas un excelente día!\n\nAtentamente,\nTu equipo de ventas\n\n\n\n\n\n"
    # Lógica para enviar la notificación
    print(mensaje)


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("KafkaIntegration") \
        .master("local[3]") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1-p:9092, kafka2-p:9092, kafka3-p:9092") \
        .option("subscribe", "DataTopic") \
        .option("startingOffsets", "earliest") \
        .load()

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

    parsed_df = streaming_df \
        .select(F.col("value").cast(StringType()).alias("value")) \
        .withColumn("input", F.from_json("value", schema)) \
        .select("input.*", "input.ubicacion.*") \
        .drop("ubicacion")

    clientes_notificados_df = clientes_notificar(parsed_df)

    # Definir la consulta de escritura en archivo Parquet
    query = clientes_notificados_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "../output_data/potenciales_compras") \
        .option("checkpointLocation", "../checkpoint/potenciales_compras") \
        .trigger(processingTime="15 seconds") \
        .start()

    def enviar_notificaciones(row):
        enviar_notificacion(row.nombre, row.productovisto)

    clientes_notificados_df.foreach(enviar_notificaciones)

    query.awaitTermination()
