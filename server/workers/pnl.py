import threading
import logging
import os
from common.utils import EOF, cargar_eofs, create_dataframe
from common.communication import iniciar_nodo, obtener_query
from transformers import pipeline # type: ignore
import time

PNL = "pnl"

# -----------------------
# Nodo PNL
# -----------------------
class PnlNode:
    def __init__(self):
        self.eof_esperados = cargar_eofs()
        self.shutdown_event = threading.Event()


    def ejecutar_consulta(self, consulta_id, datos):
        match consulta_id:
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos = create_dataframe(datos)
        sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        datos['sentiment'] = datos['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        return datos
    
    def procesar_mensajes(self, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                self.eof_esperados[consulta_id] -= 1
                if self.eof_esperados[consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    enviar_func(PNL, consulta_id, EOF, mensaje, EOF)
                    self.shutdown_event.set()
                else:
                    # Asegurarse de que el ACK no se mande antes de que todo esté procesado
                    mensaje['ack']()  
            else:
                resultado = self.ejecutar_consulta(consulta_id, mensaje['body'].decode('utf-8'))
                enviar_func(PNL, consulta_id, resultado, mensaje, "")


            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

        


# -----------------------
# Ejecutando nodo pnl
# -----------------------

if __name__ == "__main__":
    pnl = PnlNode()
    iniciar_nodo(PNL, pnl, os.getenv("CONSULTAS", ""))

