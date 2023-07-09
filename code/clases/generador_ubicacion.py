from clases.generador_direccion import GeneradorDireccion
from clases.generador_ciudad import GeneradorCiudad
from clases.generador_pais import GeneradorPais

import json

class GeneradorUbicacion:
    def generar_ubicacion(self):

        gen_direccion = GeneradorDireccion()
        direccion = gen_direccion.generar_direccion()

        gen_ciudad = GeneradorCiudad()
        ciudad = gen_ciudad.generar_ciudad()

        gen_pais = GeneradorPais()
        pais = gen_pais.generar_pais()

        ubicacion = {
            "direccion": direccion,
            "ciudad": ciudad,
            "pais": pais
        }

        return ubicacion
        