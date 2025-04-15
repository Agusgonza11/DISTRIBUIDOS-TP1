import asyncio
import logging
from common.utils import create_dataframe, initialize_log, prepare_data_aggregator_consult_3
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

AGGREGATOR = "aggregator"

# -----------------------
# Nodo Filtro
# -----------------------
class AggregatorNode:
    def __init__(self):
        self.resultados_parciales = {}

    def guardar_datos(self, consulta_id, datos):
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        self.resultados_parciales[consulta_id].append(datos)

    def ejecutar_consulta(self, consulta_id):
        datos = "\n".join(self.resultados_parciales.get(consulta_id, []))
        lineas = datos.strip().split("\n")
        logging.info(f"Ejecutando consulta {consulta_id} con {len(lineas)} elementos")
        
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
        datos = create_dataframe(datos)
        investment_by_country = datos.groupby('country')['budget'].sum().sort_values(ascending=False)
        top_5_countries = investment_by_country.head(5)
        csv_q2 = top_5_countries.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q2}")
        return csv_q2

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        datos = create_dataframe(datos)
        mean_ratings = datos.groupby(["id", "title"])['rating'].mean().reset_index()
        max_rated = mean_ratings.iloc[mean_ratings['rating'].idxmax()]
        min_rated = mean_ratings.iloc[mean_ratings['rating'].idxmin()]
        csv_q3 = prepare_data_aggregator_consult_3(min_rated, max_rated)
        logging.info(f"lo que voy a devolver es {csv_q3}")
        return csv_q3

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        datos = create_dataframe(datos)
        actor_counts = datos.groupby("name").count().reset_index().rename(columns={"id": "count"})
        top_10_actors = actor_counts.nlargest(10, 'count')
        csv_q4 = top_10_actors.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q4}")
        return csv_q4

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos = create_dataframe(datos)
        datos["rate_revenue_budget"] = datos["revenue"] / datos["budget"]
        average_rate_by_sentiment = datos.groupby("sentiment")["rate_revenue_budget"].mean()
        csv_q5 = average_rate_by_sentiment.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q5}")
        return csv_q5
    

    async def procesar_mensajes(self, destino, consulta_id, contenido, enviar_func):
        if contenido.strip() == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            resultado = self.ejecutar_consulta(consulta_id)
            await enviar_func(destino, resultado)
            await enviar_func(destino, "EOF")
            return
        self.guardar_datos(consulta_id, contenido)


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

aggregator = AggregatorNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker aggregator")
    await inicializar_comunicacion()
    await escuchar_colas(AGGREGATOR, aggregator)
    #await enviar_mock() Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

