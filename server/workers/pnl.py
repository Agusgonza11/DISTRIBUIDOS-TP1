import threading
import logging
import os
from common.utils import EOF, cargar_eofs, concat_data, create_dataframe
from common.communication import iniciar_nodo, obtener_query
from transformers import pipeline # type: ignore
import time

PNL = "pnl"

# -----------------------
# Nodo PNL
# -----------------------
class PnlNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.lineas_actuales = 0
        self.eof_esperados = cargar_eofs()
        self.shutdown_event = threading.Event()

    def guardar_datos(self, consulta_id, datos):
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        data = create_dataframe(datos)
        self.resultados_parciales[consulta_id].append(data)
        self.lineas_actuales += len(data)



    def ejecutar_consulta(self, consulta_id):
        datos = self.resultados_parciales.get(consulta_id, [])
        self.resultados_parciales[consulta_id] = []
        self.lineas_actuales = 0
        if not datos:
            return False
        datos = concat_data(datos)
        match consulta_id:
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english', truncation=True)
        datos['sentiment'] = datos['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        return datos
    
    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} de pnl recibió EOF {self.eof_esperados}")
                self.eof_esperados[consulta_id] -= 1
                if self.eof_esperados[consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    resultado = self.ejecutar_consulta(consulta_id)
                    self.resultados_parciales[consulta_id] = []
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    enviar_func(canal, destino, EOF, mensaje, EOF)
                    logging.info(f"voy a cerrar el nodo")
                    self.shutdown_event.set()
                    logging.info(f"lo cerre")
            else:
                contenido = mensaje['body'].decode('utf-8')
                self.guardar_datos(consulta_id, contenido)
            if self.lineas_actuales >= 500:
                resultado = self.ejecutar_consulta(consulta_id)
                self.resultados_parciales[consulta_id] = []
                enviar_func(canal, destino, resultado, mensaje, "RESULT")

            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

        


# -----------------------
# Ejecutando nodo pnl
# -----------------------

if __name__ == "__main__":
    pnl = PnlNode()
    iniciar_nodo(PNL, pnl, os.getenv("CONSULTAS", ""))
    logging.info(f"aca llega")
    pnl.shutdown_event.wait()
    logging.info(f"Shutdown del nodo {PNL}")
