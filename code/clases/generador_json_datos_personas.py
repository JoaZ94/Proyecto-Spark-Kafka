from clases.generador_id import GeneradorId
from clases.generador_nombre import GeneradorNombre
from clases.generador_apellido import GeneradorApellido
from clases.generador_edad import GeneradorEdad
from clases.generador_numero_telefonico import GeneradorNumeroTelefonico
from clases.generador_ubicacion import GeneradorUbicacion
from clases.generador_producto_visto import GeneradorProductoVisto
from clases.generador_taps import GeneradorTAPS

import json

class GeneradorJSONDatosPersonas():
    def generar_json_datos_personas(self):

        gen_id = GeneradorId()
        id = gen_id.generar_id()

        gen_nombre = GeneradorNombre()
        nombre = gen_nombre.generar_nombre()

        gen_apellido = GeneradorApellido()
        apellido = gen_apellido.generar_apellido()

        gen_edad = GeneradorEdad()
        edad = gen_edad.generar_edad()

        gen_telefono = GeneradorNumeroTelefonico()
        telefono = gen_telefono.generar_numero_telefonico()

        gen_ubicacion = GeneradorUbicacion()
        ubicacion = gen_ubicacion.generar_ubicacion()

        gen_producto_visto = GeneradorProductoVisto()
        producto_visto = gen_producto_visto.generar_producto_visto()

        gen_taps = GeneradorTAPS()
        taps = gen_taps.generar_taps()

        email = f"{nombre}.{apellido}@mail.com"

        datos_persona = {
            "id": id,
            "nombre": nombre,
            "apellido": apellido,
            "edad": edad,
            "email": email,
            "telefono": telefono,
            "ubicacion": ubicacion,
            "productovisto": producto_visto,
            "taps": taps
        }
        
        datos_persona_json = json.dumps(datos_persona)
        return datos_persona_json