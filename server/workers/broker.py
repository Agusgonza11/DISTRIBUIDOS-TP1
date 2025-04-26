import threading
import logging
import os
from common.utils import EOF, cargar_eofs
from common.communication import iniciar_nodo, obtener_query

BROKER = "broker"

# -----------------------
# Broker
# -----------------------
class Broker:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = threading.Event()
        self.joiners_destino = cargar_eofs()


    def distribuir_informacion(self, consulta_id, mensaje, canal, enviar_func, tipo=None):
        for joiner_id in range(1, self.joiners_destino[consulta_id] + 1):
            destino = f'joiner_consult_{consulta_id}_{joiner_id}'
            enviar_func(canal, destino, mensaje['body'].decode('utf-8'), mensaje, tipo)


    def procesar_mensajes(self, canal, _, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            tipo = EOF if mensaje['headers'].get("type") == EOF else "MOVIES"
            self.distribuir_informacion(consulta_id, mensaje, canal, enviar_func, tipo)
            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando broker
# -----------------------

if __name__ == "__main__":
    broker = Broker()
    iniciar_nodo(BROKER, broker, None, (cargar_eofs()[3], cargar_eofs()[4]))

