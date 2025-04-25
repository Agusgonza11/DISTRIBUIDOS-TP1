import threading
import logging
import os
from common.utils import cargar_eofs, create_dataframe
from common.communication import iniciar_nodo
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
    
def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
    try:
        if mensaje['headers'].get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.eof_esperados[consulta_id] -= 1
            if self.eof_esperados[consulta_id] == 0:
                logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                enviar_func(destino, "EOF", headers={"type": "EOF", "Query": consulta_id, "ClientID": mensaje['headers'].get("ClientID")})
                self.shutdown_event.set()
            else:
                # Asegurarse de que el ACK no se mande antes de que todo esté procesado
                mensaje['ack']()  
        else:
            resultado = self.ejecutar_consulta(consulta_id, mensaje['body'].decode('utf-8'))
            enviar_func(destino, resultado, headers={"Query": consulta_id, "ClientID": mensaje['headers'].get("ClientID")})

        mensaje['ack']()

    except Exception as e:
        logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

    


# -----------------------
# Ejecutando nodo pnl
# -----------------------

if __name__ == "__main__":
    pnl = PnlNode()
    iniciar_nodo(PNL, pnl, os.getenv("CONSULTAS", ""))

