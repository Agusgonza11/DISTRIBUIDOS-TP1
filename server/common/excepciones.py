# exceptions.py

class ConsultaInexistente(Exception):
    """Se lanza cuando se intenta acceder a una consulta que no existe."""
    def __init__(self, mensaje="La consulta solicitada no existe."):
        super().__init__(mensaje)
