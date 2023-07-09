import random

class GeneradorPais:
    def generar_pais(self):
        with open("../generator_data/paises.txt", "r") as archivo:
            paises = archivo.read().splitlines()
        return random.choice(paises)