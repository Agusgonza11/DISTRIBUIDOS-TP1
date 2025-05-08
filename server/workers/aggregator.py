import logging
import os
import gc
import sys
from common.utils import EOF, cargar_eofs, concat_data, create_dataframe, prepare_data_aggregator_consult_3
from common.communication import iniciar_nodo, obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje
import tracemalloc

AGGREGATOR = "aggregator"

# -----------------------
# Nodo Aggregator
# -----------------------
class AggregatorNode:
    def __init__(self):
        tracemalloc.start()
        self.resultados_parciales = {}
        self.eof_esperados = {}

    def eliminar(self):
        self.resultados_parciales = {}
        self.eof_esperados = {}

    def guardar_datos(self, consulta_id, datos, client_id):
        if client_id not in self.resultados_parciales:
            self.resultados_parciales[client_id] = {}
            self.eof_esperados[client_id] = {}
            
        if consulta_id not in self.resultados_parciales[client_id]:
            self.resultados_parciales[client_id][consulta_id] = []
            
        if consulta_id not in self.eof_esperados[client_id]:
            self.eof_esperados[client_id][consulta_id] = cargar_eofs()[consulta_id]

        self.resultados_parciales[client_id][consulta_id].append(create_dataframe(datos))

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
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

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


            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando nodo aggregator
# -----------------------

if __name__ == "__main__":
    aggregator = AggregatorNode()
    iniciar_nodo(AGGREGATOR, aggregator, os.getenv("CONSULTAS", ""))
