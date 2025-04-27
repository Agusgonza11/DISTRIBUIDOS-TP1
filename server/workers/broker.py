import threading
import logging
import os
from common.utils import EOF, cargar_datos_broker, cargar_eofs, cargar_eofs_joiners
from common.communication import iniciar_nodo, obtener_query

BROKER = "broker"

# -----------------------
# Broker
# -----------------------
class Broker:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = threading.Event()
        self.nodos_enviar = cargar_datos_broker()
        self.eof_esperar = cargar_eofs()


    def distribuir_informacion(self, consulta_id, mensaje, canal, enviar_func, tipo=None):
        for joiner_id in self.nodos_enviar[consulta_id]:
            destino = f'joiner_consult_{consulta_id}_{joiner_id}'
            enviar_func(canal, destino, mensaje['body'].decode('utf-8'), mensaje, tipo)


    def procesar_mensajes(self, canal, _, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                self.eof_esperar[consulta_id] -= 1
                if self.eof_esperar[consulta_id] == 0:
                    self.distribuir_informacion(consulta_id, mensaje, canal, enviar_func, EOF)
            else:
                self.distribuir_informacion(consulta_id, mensaje, canal, enviar_func, "MOVIES")
            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando broker
# -----------------------

if __name__ == "__main__":
    broker = Broker()
    iniciar_nodo(BROKER, broker)

