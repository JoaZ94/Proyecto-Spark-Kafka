import random

class GeneradorNombre:
    def generar_nombre(self):
        with open("../generator_data/nombres.txt", "r") as archivo:
            nombres = archivo.read().splitlines()
        return random.choice(nombres)