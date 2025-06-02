import logging
from multiprocessing import Process
import os
import gc
import pickle
import sys

import pandas as pd
from common.utils import EOF, cargar_eofs, concat_data, create_dataframe, fue_reiniciado, obtiene_nombre_contenedor, prepare_data_aggregator_consult_3
from common.communication import iniciar_nodo, obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje
import tracemalloc
from common.excepciones import ConsultaInexistente
from common.health import HealthMonitor


AGGREGATOR = "aggregator"

# -----------------------
# Nodo Aggregator
# -----------------------
class AggregatorNode:
    def __init__(self, reiniciado=None):
        tracemalloc.start()
        self.resultados_parciales = {}
        self.resultados_health = {}
        self.eof_esperados = {}
        self.health_file = f"/app/reinicio_flags/{obtiene_nombre_contenedor(AGGREGATOR)}.data"
        if reiniciado:
            self.cargar_estado()

    def eliminar(self):
        self.resultados_parciales = {}
        self.eof_esperados = {}


    def cargar_estado(self):
        try:
            with open(self.health_file, "rb") as f:
                estado = pickle.load(f)
                self.resultados_health = estado.get("resultados_parciales", {})
                self.eof_esperados = estado.get("eof_esperados", {})
                self.resultados_parciales = {}

                # Recrear los DataFrames desde los datos crudos
                for client_id, consultas in self.resultados_health.items():
                    self.resultados_parciales[client_id] = {}
                    for consulta_id, lista_datos in consultas.items():
                        # Aplicar create_dataframe a cada entrada individual
                        dfs = [create_dataframe(datos) for datos in lista_datos]
                        self.resultados_parciales[client_id][consulta_id] = dfs
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)


    def guardar_estado(self):
        try:
            with open(self.health_file, "wb") as f:
                pickle.dump({
                    "resultados_parciales": self.resultados_health,
                    "eof_esperados": self.eof_esperados
                }, f)
        except Exception as e:
            print(f"Error al guardar el estado: {e}", flush=True)


    def guardar_datos(self, consulta_id, datos, client_id):
        if client_id not in self.resultados_parciales:
            self.resultados_parciales[client_id] = {}
            self.resultados_health[client_id] = {}
            self.eof_esperados[client_id] = {}
            
        if consulta_id not in self.resultados_parciales[client_id]:
            self.resultados_parciales[client_id][consulta_id] = []
            self.resultados_health[client_id][consulta_id] = []
            
        if consulta_id not in self.eof_esperados[client_id]:
            self.eof_esperados[client_id][consulta_id] = cargar_eofs()[consulta_id]

        self.resultados_parciales[client_id][consulta_id].append(create_dataframe(datos))
        self.resultados_health[client_id][consulta_id].append(datos)

        self.guardar_estado()



    def ejecutar_consulta(self, consulta_id, client_id):
        if client_id not in self.resultados_parciales:
            return False
        
        datos_cliente = self.resultados_parciales[client_id]
        if not datos_cliente:
            return False 
        
        datos = concat_data(datos_cliente[consulta_id])
        
        logging.info(tracemalloc.get_traced_memory())
        tracemalloc.stop()
        logging.info("Memoria usada: \n")
        size_bytes = sys.getsizeof(datos)
        size_mb = size_bytes / (1024 ** 2)
        logging.info(f"Uso de memoria: {size_mb:.2f} MB")
        
        match consulta_id:
            case 2:
                return self.consulta_2(datos)
            case 3:
                return self.consulta_3(datos)
            case 4:
                return self.consulta_4(datos)
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida {consulta_id}")
                raise ConsultaInexistente(f"Consulta {consulta_id} no encontrada")
    

    def consulta_2(self, datos):
        logging.info("Procesando datos para consulta 2")
        investment_by_country = datos.groupby('country')['budget'].sum().sort_values(ascending=False)
        top_5_countries = investment_by_country.head(5).reset_index()
        return top_5_countries

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        logging.info(f"Datos recibidos con shape: {datos.shape}")
        
        mean_ratings = datos.groupby(["id", "title"])['rating'].mean().reset_index()
        max_rated = mean_ratings.iloc[mean_ratings['rating'].idxmax()]
        min_rated = mean_ratings.iloc[mean_ratings['rating'].idxmin()]
        result = prepare_data_aggregator_consult_3(min_rated, max_rated)
        return result

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        cast_per_movie_quantities = datos.groupby(["name"]).count().reset_index().rename(columns={"id":"count"})
        top_ten = cast_per_movie_quantities.nlargest(10, 'count')
        return top_ten

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos["rate_revenue_budget"] = datos["revenue"] / datos["budget"]
        average_rate_by_sentiment = datos.groupby("sentiment")["rate_revenue_budget"].mean().reset_index()
        return average_rate_by_sentiment
    

    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        try:
            if tipo_mensaje == EOF:
                logging.info(f"Consulta {consulta_id} de aggregator recibió EOF")
                self.eof_esperados[client_id][consulta_id] -= 1
                if self.eof_esperados[client_id][consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    resultado = self.ejecutar_consulta(consulta_id, client_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    enviar_func(canal, destino, EOF, mensaje, EOF)
                    del self.resultados_parciales[client_id][consulta_id]
                    del self.eof_esperados[client_id][consulta_id]

                    if not self.resultados_parciales[client_id]:
                        del self.resultados_parciales[client_id]
                    if not self.eof_esperados[client_id]:
                        del self.eof_esperados[client_id]

                    gc.collect()
            else:
                self.guardar_datos(consulta_id, obtener_body(mensaje), client_id)
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")
        mensaje['ack']()



# -----------------------
# Ejecutando nodo aggregator
# -----------------------

if __name__ == "__main__":
    reiniciado = False
    if fue_reiniciado(AGGREGATOR):
        print("El nodo fue reiniciado", flush=True)
        reiniciado = True
    proceso_nodo = Process(target=iniciar_nodo, args=(AGGREGATOR, AggregatorNode(reiniciado), os.getenv("CONSULTAS", "")))
    monitor = HealthMonitor(AGGREGATOR)
    proceso_monitor = Process(target=monitor.run)

    proceso_nodo.start()
    proceso_monitor.start()

    proceso_nodo.join()
    proceso_monitor.join()
