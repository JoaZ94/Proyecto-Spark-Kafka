import random
import string

class GeneradorId:
    def generar_id(self):
        caracteres = string.hexdigits[:-6].lower()  # No usa letras mayúsculas de 'ABCDEF'
        identificador = ''.join(random.choice(caracteres) for _ in range(24))
        return identificador
    