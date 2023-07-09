import random

class GeneradorDireccion:
    def generar_direccion(self):
        with open("../generator_data/direcciones.txt", "r") as archivo:
            direcciones = archivo.read().splitlines()
        return random.choice(direcciones)