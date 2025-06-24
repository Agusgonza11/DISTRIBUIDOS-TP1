# exceptions.py

class ConsultaInexistente(Exception):
    """Se lanza cuando se intenta acceder a una consulta que no existe."""
    def __init__(self, mensaje="La consulta solicitada no existe."):
        super().__init__(mensaje)

class ErrorCargaDelEstado(Exception):
    """Se lanza cuando hubo un error en la carga del estado de un nodo"""
    def __init__(self, mensaje="Error en la carga del estado"):
        super().__init__(mensaje)
