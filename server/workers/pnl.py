import threading
import logging
import os
from common.utils import EOF, concat_data, create_dataframe
from common.communication import iniciar_nodo, obtener_query
from transformers import pipeline # type: ignore
import time

PNL = "pnl"
BATCH = 200

# -----------------------
# Nodo PNL
# -----------------------
class PnlNode:
    def __init__(self):
        self.resultados_parciales = []
        self.lineas_actuales = 0

    def guardar_datos(self, datos):
        data = create_dataframe(datos)
        self.resultados_parciales.append(data)
        self.lineas_actuales += len(data)

    def borrar_info(self):
        self.resultados_parciales = []
        self.lineas_actuales = 0

    def ejecutar_consulta(self, consulta_id):
        datos = self.resultados_parciales
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
        self.borrar_info()
        return datos
    
    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} de pnl recibió EOF")
                if self.resultados_parciales:
                    resultado = self.ejecutar_consulta(consulta_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                enviar_func(canal, destino, EOF, mensaje, EOF)
            else:
                contenido = mensaje['body'].decode('utf-8')
                self.guardar_datos(contenido)
                if self.lineas_actuales >= BATCH:
                    resultado = self.ejecutar_consulta(consulta_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")

            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

        


# -----------------------
# Ejecutando nodo pnl
# -----------------------

if __name__ == "__main__":
    pnl = PnlNode()
    worker_id = int(os.environ.get("WORKER_ID", 0))
    iniciar_nodo(PNL, pnl, os.getenv("CONSULTAS", ""), worker_id)