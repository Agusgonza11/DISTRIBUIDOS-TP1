import logging
from multiprocessing import Process
import os
import pickle
import sys
from common.utils import EOF, borrar_contenido_carpeta, concat_data, create_dataframe, fue_reiniciado, get_batches, obtiene_nombre_contenedor
from common.communication import iniciar_nodo, obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from transformers import pipeline # type: ignore
import torch # type: ignore
from common.excepciones import ConsultaInexistente


PNL = "pnl"
BATCH_PNL = get_batches(PNL)
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
torch.set_num_threads(1)


# -----------------------
# Nodo PNL
# -----------------------
class PnlNode:
    def __init__(self, reiniciado=None):
        self.sentiment_analyzer = pipeline(
            'sentiment-analysis',
            model='distilbert-base-uncased-finetuned-sst-2-english',
            truncation=True
        )
        self.resultados_parciales = {}
        self.lineas_actuales = {}
        self.resultados_health = {}
        self.modifico = False
        self.health_file = f"/app/reinicio_flags/{obtiene_nombre_contenedor(PNL)}.data"
        if reiniciado:
            self.cargar_estado()

    def cargar_estado(self):
        try:
            with open(self.health_file, "rb") as f:
                estado = pickle.load(f)
                self.resultados_health = estado.get("resultados_parciales", {})
                self.lineas_actuales = estado.get("lineas_actuales", {})
                self.resultados_parciales = {}

                # Recrear los DataFrames desde los datos crudos
                for client_id, lista_datos in self.resultados_health.items():
                    self.resultados_parciales[client_id] = [
                        create_dataframe(datos) for datos in lista_datos
                    ]
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)


    def guardar_estado(self):
        if not self.modifico:
            return
        try:
            with open(self.health_file, "wb") as f:
                pickle.dump({
                    "resultados_parciales": self.resultados_health,
                    "lineas_actuales": self.lineas_actuales
                }, f)
        except Exception as e:
            print(f"Error al guardar el estado: {e}", flush=True)


    def eliminar(self, es_global):
        self.resultados_parciales = {}
        self.lineas_actuales = {}
        self.resultados_health = {}
        self.cambios = {}
        if hasattr(self, 'sentiment_analyzer'):
            del self.sentiment_analyzer
        if es_global:
            try:
                borrar_contenido_carpeta()
                logging.info(f"Volumen limpiado por shutdown global")
            except Exception as e:
                logging.error(f"Error limpiando volumen en shutdown global: {e}")

    def guardar_datos(self, datos, client_id):
        if not client_id in self.resultados_parciales:
            self.resultados_parciales[client_id] = []
            self.resultados_health[client_id] = []
            self.lineas_actuales[client_id] = 0
        data = create_dataframe(datos)
        self.resultados_parciales[client_id].append(data)
        self.resultados_health[client_id].append(datos)
        self.lineas_actuales[client_id] += len(data)

    def borrar_info(self, client_id):
        self.resultados_parciales[client_id] = []
        self.resultados_health[client_id] = []
        self.lineas_actuales[client_id] = 0

    def ejecutar_consulta(self, consulta_id, client_id):
        if client_id not in self.resultados_parciales:
            return False
        
        datos_cliente = self.resultados_parciales[client_id]
        if not datos_cliente:
            return False 
        
        datos = concat_data(datos_cliente)

        match consulta_id:
            case 5:
                return self.consulta_5(datos, client_id)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                raise ConsultaInexistente(f"Consulta {consulta_id} no encontrada")
    

    def consulta_5(self, datos, client_id):
        logging.info("Procesando datos para consulta 5")
        overviews = datos['overview'].fillna('').tolist()
        sentiments = self.sentiment_analyzer(overviews, truncation=True)
        datos['sentiment'] = [result['label'] for result in sentiments]

        self.borrar_info(client_id)
        return datos
    
    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        client_id = obtener_client_id(mensaje)
        try:
            if obtener_tipo_mensaje(mensaje) == EOF:
                logging.info(f"Consulta {consulta_id} de pnl recibió EOF")
                if self.resultados_parciales[client_id]:
                    resultado = self.ejecutar_consulta(consulta_id, client_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                enviar_func(canal, destino, EOF, mensaje, EOF)
                self.modifico = False
            else:
                self.guardar_datos(obtener_body(mensaje), client_id) #guardar una variable para q solo guarde
                if self.lineas_actuales[client_id] >= BATCH_PNL:
                    resultado = self.ejecutar_consulta(consulta_id, client_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                self.modifico = True
            self.guardar_estado()
            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

        


# -----------------------
# Ejecutando nodo pnl
# -----------------------

if __name__ == "__main__":
    run(PNL, PnlNode)

