import random

class GeneradorProductoVisto:
    def generar_producto_visto(self):
        with open("../generator_data/productos.txt", "r") as archivo:
            productos = archivo.read().splitlines()
        return random.choice(productos)