import random

class GeneradorCiudad:
    def generar_ciudad(self):
        with open("../generator_data/ciudades.txt", "r") as archivo:
            ciudades = archivo.read().splitlines()
        return random.choice(ciudades)