import asyncio
import logging
import os
from common.utils import cargar_eofs, concat_data, create_dataframe, initialize_log, prepare_data_aggregator_consult_3
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

AGGREGATOR = "aggregator"

# -----------------------
# Nodo Aggregator
# -----------------------
class AggregatorNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = asyncio.Event()
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
        logging.info(f"Ejecutando consulta {consulta_id} con {len(datos)} elementos")        
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
        logging.info(f"Lo que me llega es {datos}")
        actor_counts = datos.groupby("cast").count().reset_index().rename(columns={"id": "count"})
        top_10_actors = actor_counts.nlargest(10, 'count')
        return top_10_actors

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos["rate_revenue_budget"] = datos["revenue"] / datos["budget"]
        average_rate_by_sentiment = datos.groupby("sentiment")["rate_revenue_budget"].mean().reset_index()
        return average_rate_by_sentiment
    

    async def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
        if mensaje.headers.get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.eof_esperados[consulta_id] -= 1
            if self.eof_esperados[consulta_id] == 0:
                logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                resultado = self.ejecutar_consulta(consulta_id)
                await enviar_func(destino, resultado, headers={"type": "RESULT", "Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")})
                self.shutdown_event.set()
                return
        contenido = mensaje.body.decode('utf-8') 
        self.guardar_datos(consulta_id, contenido)


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

aggregator = AggregatorNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker aggregator")
    consultas_str = os.getenv("CONSULTAS", "")
    consultas = list(map(int, consultas_str.split(","))) if consultas_str else []

    await inicializar_comunicacion()
    await escuchar_colas(AGGREGATOR, aggregator, consultas)
    #await enviar_mock() # Mock para probar consultas
    await aggregator.shutdown_event.wait()
    logging.info("Shutdown del nodo aggregator")

asyncio.run(main())

