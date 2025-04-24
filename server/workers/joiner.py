import asyncio
import logging
import os
from common.utils import cargar_eofs, concat_data, create_dataframe, initialize_log, prepare_data_aggregator_consult_3
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas
from db.db_client import DBClient
import pandas as pd # type: ignore


JOINER = "joiner"

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = asyncio.Event()
        self.eof_esperados = cargar_eofs()
        self.termino_credits = False
        self.termino_movies = False
        self.termino_ratings = False
        self.db_client = DBClient()
        self.lineas_csv = {"ratings": 0, "credits": 0}
        self.umbral_envio = 100


    def guardar_csv(self, csv, datos):
        logging.info(f"entro a guardar {csv} los datos {datos}")

        cuerpo = datos.split('\n', 1)[1]
        self.db_client.guardar_csv(csv, cuerpo)
        lineas = cuerpo.strip().split("\n")
        self.lineas_csv[csv] += len(lineas)



    def puede_enviar(self, consulta_id):
        puede_enviar = False
        if consulta_id == 3 and self.lineas_csv["ratings"] >= self.umbral_envio:
            self.lineas_csv["ratings"] = 0
            puede_enviar = True
        if consulta_id == 4 and self.lineas_csv["credits"] >= self.umbral_envio:
            self.lineas_csv["credits"] = 0
            puede_enviar = True        
        return puede_enviar


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
            case 3:
                return self.consulta_3(datos)
            case 4:
                return self.consulta_4(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        ratings = self.db_client.obtener_ratings()
        if not ratings:
            return False
        logging.info(f"BUSCAMOS RATINGS y obtuvimos {ratings}")

        ratings_df = pd.DataFrame(ratings, columns=["id", "rating"])
        ratings_df.rename(columns={"movieId": "id"}, inplace=True)

        return datos.merge(ratings_df, on="id")

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        #Consulta 4 Tomas
        return datos
    
    def consulta_completa(self, consulta_id):
        match consulta_id:
            case 3:
                return self.termino_credits and self.termino_movies
            case 4:
                return self.termino_ratings and self.termino_movies
            case _:
                return self.termino_movies

    def termino_nodo(self):
        return self.termino_credits and self.termino_movies and self.termino_ratings
    
    async def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
        if mensaje.headers.get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.eof_esperados[consulta_id] -= 1
            if self.eof_esperados[consulta_id] == 0:
                logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                self.termino_movies = True
                return
        if mensaje.headers.get("EOF_RATINGS"):
            logging.info("recibe el eof ratings")
            self.termino_ratings = True
        if mensaje.headers.get("EOF_CREDITS"):
            logging.info("recibe el eof credits")
            self.termino_credits = True
        if mensaje.headers.get("type") == "RATINGS":
            self.guardar_csv("ratings", mensaje.body.decode('utf-8'))
        if mensaje.headers.get("type") == "CREDITS":
            self.guardar_csv("credits", mensaje.body.decode('utf-8'))
        if mensaje.headers.get("type") == "MOVIES":
            self.guardar_datos(consulta_id, mensaje.body.decode('utf-8'))
        if self.termino_movies and self.puede_enviar(consulta_id):
            resultado = self.ejecutar_consulta(consulta_id)
            await enviar_func(destino, resultado, headers={"Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")})
        if self.consulta_completa(consulta_id):
            await enviar_func(destino, "EOF", headers={"type": "EOF", "Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")})
        if self.termino_nodo():
            self.shutdown_event.set()


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

joiner = JoinerNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker joiner")
    consultas_str = os.getenv("CONSULTAS", "")
    consultas = list(map(int, consultas_str.split(","))) if consultas_str else []

    await inicializar_comunicacion()
    await escuchar_colas(JOINER, joiner, consultas)
    #await enviar_mock() # Mock para probar consultas
    await joiner.shutdown_event.wait()
    logging.info("Shutdown del nodo joiner")

asyncio.run(main())

