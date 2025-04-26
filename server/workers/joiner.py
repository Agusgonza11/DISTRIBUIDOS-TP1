import threading
import logging
import os
from common.utils import EOF, cargar_eofs, concat_data, create_dataframe, prepare_data_consult_4
from common.communication import iniciar_nodo, obtener_query
import pandas as pd # type: ignore


JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:
    def __init__(self, consultas):
        self.resultados_parciales = {}
        self.consultas = consultas
        self.shutdown_event = threading.Event()
        self.eof_esperados = cargar_eofs()
        self.termino_movies = False
        self.umbral_envio_ratings = 100000
        self.umbral_envio_credits = 10000
        self.datos = {"ratings": [[], 0, False], "credits": [[], 0, False]}
        # "CSV": (datos, cantidad de datos, termino de recibir todo)

    def puede_enviar(self, consulta_id):
        puede_enviar = False
        if consulta_id == 3 and (self.datos["ratings"][LINEAS] >= self.umbral_envio_ratings or self.datos["ratings"][TERMINO]):
            puede_enviar = True
        if consulta_id == 4 and (self.datos["credits"][LINEAS] >= self.umbral_envio_credits or self.datos["credits"][TERMINO]):
            puede_enviar = True        
        return puede_enviar


    def guardar_datos(self, consulta_id, datos):
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        self.resultados_parciales[consulta_id].append(create_dataframe(datos))


    def almacenar_csv(self, consulta, datos):
        if consulta == 3:
            csv = "ratings"
        else:
            csv = "credits"
        self.datos[csv][LINEAS] += len(datos)
        self.datos[csv][DATOS].append(create_dataframe(datos))


    def ejecutar_consulta(self, consulta_id):
        datos = self.resultados_parciales.get(consulta_id, [])
        if not datos:
            return False

        datos = concat_data(datos)
        
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
        ratings = concat_data(self.datos["ratings"][DATOS])
        self.datos["ratings"][DATOS] = []
        self.datos["ratings"][LINEAS] = 0
        if ratings.empty:
            return False
        ranking_arg_post_2000_df = datos[["id", "title"]].merge(ratings, on="id")
        return ranking_arg_post_2000_df

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        credits = prepare_data_consult_4(self.datos["credits"][DATOS])
        self.datos["credits"][DATOS] = []
        self.datos["credits"][LINEAS] = 0        
        if credits.empty:
            return False
        cast_arg_post_2000_df = datos[["id", "title"]].merge(credits, on="id")
        df_cast = cast_arg_post_2000_df.explode('cast')

        # Te quedás con id y nombre del actor
        cast_and_movie_arg_post_2000_df = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})

        return cast_and_movie_arg_post_2000_df

    
    def consulta_completa(self, consulta_id):
        match consulta_id:
            case 3:
                return self.datos["ratings"][TERMINO] and self.termino_movies
            case 4:
                return self.datos["credits"][TERMINO] and self.termino_movies
            case _:
                return self.termino_movies


    def termino_nodo(self):
        if 3 in self.consultas and 4 in self.consultas:
            termino_nodo = True if self.termino_movies and self.datos["ratings"][TERMINO] and self.datos["credits"][TERMINO] else False
        if 3 in self.consultas and 4 not in self.consultas:
            termino_nodo = True if self.termino_movies and self.datos["ratings"][TERMINO] else False
        if 3 not in self.consultas and 4 in self.consultas:
            termino_nodo = True if self.termino_movies and self.datos["credits"][TERMINO] else False
        return termino_nodo
    
    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                self.eof_esperados[consulta_id] -= 1
                if self.eof_esperados[consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba de movies")
                    self.termino_movies = True

            if mensaje['headers'].get("type") == "EOF_RATINGS":
                logging.info(f"Recibí todos los ratings")
                self.datos["ratings"][TERMINO] = True

            if mensaje['headers'].get("type") == "EOF_CREDITS":
                logging.info(f"Recibí todos los credits")
                self.datos["credits"][TERMINO] = True

            if mensaje['headers'].get("type") == "RATINGS":
                self.almacenar_csv(consulta_id, mensaje['body'].decode('utf-8'))

            if mensaje['headers'].get("type") == "CREDITS":
                self.almacenar_csv(consulta_id, mensaje['body'].decode('utf-8'))

            if mensaje['headers'].get("type") == "MOVIES":
                self.guardar_datos(consulta_id, mensaje['body'].decode('utf-8'))

            if self.termino_movies and self.puede_enviar(consulta_id):
                resultado = self.ejecutar_consulta(consulta_id)
                enviar_func(canal, destino, resultado, mensaje, "")
            
            if self.consulta_completa(consulta_id):
                enviar_func(canal, destino, EOF, mensaje, EOF)

            if self.termino_nodo():
                self.shutdown_event.set()
            
            mensaje['ack']() 
        except Exception as e:
            logging.error(f"Error procesando mensaje para consulta {consulta_id}: {e}")
            # No se hace ack en caso de error, lo que permitirá reintentar el mensaje


# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    consultas = os.getenv("CONSULTAS", "")
    worker_id = int(os.environ.get("WORKER_ID", 0))
    consultas_atiende = list(map(int, consultas.split(","))) if consultas else []
    joiner = JoinerNode(consultas_atiende)
    iniciar_nodo(JOINER, joiner, consultas, None, worker_id)

