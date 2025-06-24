import logging
from multiprocessing import Process
import os
import sys
from common.utils import EOF, concat_data, create_dataframe, get_batch_limits, obtener_message_id, obtener_nombre_contenedor, parse_datos
from common.communication import obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from transformers import pipeline # type: ignore
import torch # type: ignore
from common.exceptions import ConsultaInexistente, ErrorCargaDelEstado
from common.transaction import ACCION, Transaction


PNL = "pnl"
TMP_DIR = f"/tmp/{obtener_nombre_contenedor(PNL)}_tmp"

BATCH_PNL = get_batch_limits(PNL)
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
torch.set_num_threads(1)

RESULT = "resultados_parciales"

NO_ENVIAR = "no_enviar"
ENVIAR = "enviar"

# -----------------------
# Nodo PNL
# -----------------------
class PnlNode:
    def __init__(self):
        self.sentiment_analyzer = pipeline(
            'sentiment-analysis',
            model='distilbert-base-uncased-finetuned-sst-2-english',
            truncation=True
        )
        self.resultados_parciales = {}
        self.lineas_actuales = {}
        self.transaction = Transaction(TMP_DIR)
        self.transaction.cargar_estado(self)

    
    def reconstruir_estado(self, _, contenido):
        client_id, _, valor, lineas = contenido.split("|", 3)
        if client_id not in self.resultados_parciales:
            self.resultados_parciales[client_id] = []
        if valor == "BORRADO":
            self.resultados_parciales[client_id] = []
            self.lineas_actuales[client_id] = 0
        else:
            self.resultados_parciales[client_id].append(parse_datos(valor))
            self.lineas_actuales[client_id] = int(lineas)


    def eliminar(self, es_global):
        self.resultados_parciales.clear()
        self.lineas_actuales.clear()
        if hasattr(self, 'sentiment_analyzer'):
            del self.sentiment_analyzer
        if es_global:
            try:
                self.transaction.borrar_carpeta()
                logging.info(f"Volumen limpiado por shutdown global")
            except Exception as e:
                logging.error(f"Error limpiando volumen en shutdown global: {e}")


    def guardar_datos(self, datos, client_id):
        if not client_id in self.resultados_parciales:
            self.resultados_parciales[client_id] = []
            self.lineas_actuales[client_id] = 0
        data = create_dataframe(datos)
        self.resultados_parciales[client_id].append(data)
        self.lineas_actuales[client_id] += len(data)
        self.transaction.commit(RESULT, [client_id, 5, data, self.lineas_actuales[client_id]])


    def borrar_info(self, client_id):
        self.resultados_parciales[client_id] = []
        self.lineas_actuales[client_id] = 0
        self.transaction.commit(RESULT, [client_id, 5, "BORRADO", 0])



    def ejecutar_consulta(self, request_id, client_id):
        if client_id not in self.resultados_parciales:
            return False
        
        datos_cliente = self.resultados_parciales[client_id]
        if not datos_cliente:
            return False 
        
        datos = concat_data(datos_cliente)

        match request_id:
            case 5:
                return self.ejecutar_consulta_5(datos, client_id)
            case _:
                logging.warning(f"Consulta desconocida: {request_id}")
                raise ConsultaInexistente(f"Consulta {request_id} no encontrada")
    

    def ejecutar_consulta_5(self, datos, client_id):
        logging.info("Procesando datos para consulta 5")
        overviews = [fila.get("overview", "") or "" for fila in datos]
        sentiments = self.sentiment_analyzer(overviews, truncation=True)
        for fila, resultado in zip(datos, sentiments):
            fila['sentiment'] = resultado.get('label', 'UNKNOWN')
        self.borrar_info(client_id)
        return datos
        

    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        request_id = obtener_query(mensaje)
        client_id = obtener_client_id(mensaje)
        message_id = obtener_message_id(mensaje)
        if self.transaction.mensaje_duplicado(client_id, message_id, enviar_func, mensaje, canal, destino):
            logging.debug(f"Mensaje {message_id} ya procesado, ignorando")
            return
        try:
            if obtener_tipo_mensaje(mensaje) == EOF:
                logging.info(f"Consulta {request_id} de pnl recibió EOF")
                if self.resultados_parciales[client_id]:
                    resultado = self.ejecutar_consulta(request_id, client_id)
                    self.transaction.commit(ACCION, [message_id, resultado, ENVIAR])
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])
                self.transaction.commit(ACCION, [message_id, EOF, ENVIAR])
                enviar_func(canal, destino, EOF, mensaje, EOF)
                self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])
            else:
                self.guardar_datos(obtener_body(mensaje), client_id)
                if self.lineas_actuales[client_id] >= BATCH_PNL:
                    resultado = self.ejecutar_consulta(request_id, client_id)
                    self.transaction.commit(ACCION, [message_id, resultado, ENVIAR])
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])
            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {request_id}: {e}")

        


# -----------------------
# Ejecutando nodo pnl
# -----------------------

if __name__ == "__main__":
    run(PNL, PnlNode)

