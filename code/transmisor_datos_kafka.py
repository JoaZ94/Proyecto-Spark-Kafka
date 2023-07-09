from clases.generador_json_datos_personas import GeneradorJSONDatosPersonas
from kafka import KafkaProducer    #pip install kafka-python
#import keyboard                    #pip install keyboard
import time

# Configuración del Kafka
bootstrap_servers = 'localhost:9092'  # Cambia esto si tu servidor Kafka tiene una configuración diferente
topic_name = 'DataTopic'  # Cambia esto por el nombre de tu topic


# Generar datos y enviar cada 3 segundos a Kafka hasta que se presione cualquier tecla para salir
print("Presiona la tecla 'q' + 'Enter' para detener la ejecución.")

while True:

    # Crear un productor Kafka
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Geneara datos (mensaje) que se enviará al topic
    gen_datos_persona = GeneradorJSONDatosPersonas()
    datos_persona = gen_datos_persona.generar_json_datos_personas()
    print(datos_persona)

    producer.send(topic_name, value=str(datos_persona).encode('utf-8'))
    # Cerrar el productor
    producer.close()
    time.sleep(3)  # Esperar 3 segundos antes de enviar el próximo mensaje
    
    # Verificar si se ha presionado alguna tecla para salir
    #if input() =='q':
    #    break
