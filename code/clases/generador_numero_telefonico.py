import random

class GeneradorNumeroTelefonico:
    def generar_numero_telefonico(self):
        codigo_pais = random.randint(1, 99)
        numero_telefonico = f"+{codigo_pais:02d} "
        for _ in range(3):
            numero_telefonico += f"{random.randint(0, 9)}"
        numero_telefonico += "-"
        for _ in range(3):
            numero_telefonico += f"{random.randint(0, 9)}"
        numero_telefonico += "-"
        for _ in range(4):
            numero_telefonico += f"{random.randint(0, 9)}"
        return numero_telefonico