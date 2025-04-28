import threading
import logging
import os
from common.utils import EOF, cargar_eofs, concat_data, create_dataframe, prepare_data_aggregator_consult_3
from common.communication import iniciar_nodo, obtener_query

AGGREGATOR = "aggregator"

# -----------------------
# Nodo Aggregator
# -----------------------
class AggregatorNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = threading.Event()
        self.eof_esperados = cargar_eofs()

    def guardar_datos(self, consulta_id, datos):
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        self.resultados_parciales[consulta_id].append(create_dataframe(datos))


    def ejecutar_consulta(self, consulta_id):
        datos = self.resultados_parciales.get(consulta_id, [])
        if not datos:
            return False
        datos = concat_data(datos)
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
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} de aggregator recibió EOF")
                self.eof_esperados[consulta_id] -= 1
                if self.eof_esperados[consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    resultado = self.ejecutar_consulta(consulta_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    self.resultados_parciales[consulta_id] = []
            else:
                contenido = mensaje['body'].decode('utf-8')
                self.guardar_datos(consulta_id, contenido)


            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando nodo aggregator
# -----------------------

if __name__ == "__main__":
    aggregator = AggregatorNode()
    iniciar_nodo(AGGREGATOR, aggregator, os.getenv("CONSULTAS", ""))

