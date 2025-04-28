import sys
import logging
import os
from common.utils import EOF, concat_data, create_dataframe, prepare_data_consult_4
from common.communication import iniciar_nodo, obtener_query
import pandas as pd # type: ignore


JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2
BATCH_CREDITS = 10000
BATCH_RATINGS = 100000

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.termino_movies = False
        self.datos = {"ratings": [[], 0, False], "credits": [[], 0, False]}
        # "CSV": (datos, cantidad de datos, recibio el EOF correspondiente)


    def puede_enviar(self, consulta_id):
        puede_enviar = False
        if consulta_id == 3 and self.datos["ratings"][LINEAS] >= BATCH_RATINGS and self.termino_movies:
            puede_enviar = True
        if consulta_id == 4 and self.datos["credits"][LINEAS] >= BATCH_CREDITS and self.termino_movies:
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
        datos = create_dataframe(datos)
        self.datos[csv][LINEAS] += len(datos)
        self.datos[csv][DATOS].append(datos)

    def borrar_info(self, csv):
        self.datos[csv][DATOS] = []
        self.datos[csv][LINEAS] = 0

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
        self.borrar_info("ratings")
        if ratings.empty:
            return False
        ranking_arg_post_2000_df = datos[["id", "title"]].merge(ratings, on="id")
        return ranking_arg_post_2000_df

    def consulta_4(self, datos):
        logging.info(f"Procesando datos para consulta 4")
        credits = prepare_data_consult_4(self.datos["credits"][DATOS])
        self.borrar_info("credits")
        if credits.empty:
            return False
        cast_arg_post_2000_df = datos[["id", "title"]].merge(credits, on="id")
        df_cast = cast_arg_post_2000_df.explode('cast')
        cast_and_movie_arg_post_2000_df = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})
        return cast_and_movie_arg_post_2000_df


    def procesar_datos(self, consulta_id, tipo_mensaje, mensaje):
        """Procesa y guarda los datos dependiendo del tipo de mensaje."""
        if tipo_mensaje == "MOVIES":
            self.guardar_datos(consulta_id, mensaje['body'].decode('utf-8'))
        else:
            self.almacenar_csv(consulta_id, mensaje['body'].decode('utf-8'))


    def procesar_resultado(self, consulta_id, canal, destino, mensaje, enviar_func):
        """Envía el resultado si es posible."""
        if self.puede_enviar(consulta_id):
            resultado = self.ejecutar_consulta(consulta_id)
            enviar_func(canal, destino, resultado, mensaje, "RESULT")
    
    def enviar_eof(self, consulta_id, canal, destino, mensaje, enviar_func):
        if self.termino_movies and (
            (consulta_id == 3 and self.datos["ratings"][TERMINO]) or 
            (consulta_id == 4 and self.datos["credits"][TERMINO])
        ):
            enviar_func(canal, destino, EOF, mensaje, EOF)



    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = mensaje['headers'].get("type")

        if tipo_mensaje == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.termino_movies = True
            self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func)
            self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func)
        
        elif tipo_mensaje in ["MOVIES", "RATINGS", "CREDITS"]:
            self.procesar_datos(consulta_id, tipo_mensaje, mensaje)
            if tipo_mensaje != "MOVIES":
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func)

        elif tipo_mensaje == "EOF_RATINGS":
            logging.info("Recibí todos los ratings")
            self.datos["ratings"][TERMINO] = True
            if self.datos["ratings"][LINEAS] != 0:
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func)
            self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func)


        elif tipo_mensaje == "EOF_CREDITS":
            logging.info("Recibí todos los credits")
            self.datos["credits"][TERMINO] = True
            if self.datos["credits"][LINEAS] != 0:
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func)
            self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func)

        mensaje['ack']() 


# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    consultas = os.getenv("CONSULTAS", "")
    worker_id = int(os.environ.get("WORKER_ID", 0))
    joiner = JoinerNode()
    iniciar_nodo(JOINER, joiner, consultas, worker_id)
