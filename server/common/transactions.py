

class Transaction:
    def __init__(self):
        self.cambios = {}
    
    def modificar(self, claves):
        for clave in claves:
            self.cambios[clave] = True