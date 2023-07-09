import random

class GeneradorApellido:
    def generar_apellido(self):
        with open("../generator_data/apellidos.txt", "r") as archivo:
            apellidos = archivo.read().splitlines()
        return random.choice(apellidos)