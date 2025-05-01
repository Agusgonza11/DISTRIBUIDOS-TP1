import sys
import logging
import os
from common.utils import EOF, concat_data, create_dataframe, prepare_data_consult_4
from common.communication import iniciar_nodo, obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje
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
        self.termino_movies = {}
        self.datos = {}

    def eliminar(self):
        self.resultados_parciales = {}
        self.termino_movies = {}
        self.datos = {}

    def crear_datos(self, client_id):
        self.datos[client_id] = {"ratings": [[], 0, False], "credits": [[], 0, False]}
        # "CSV": (datos, cantidad de datos, recibio el EOF correspondiente)
        self.termino_movies[client_id] = False
        self.resultados_parciales[client_id] = {}

    def puede_enviar(self, consulta_id, client_id):
        if consulta_id == 3 and self.datos[client_id]["ratings"][LINEAS] >= BATCH_RATINGS and self.termino_movies[client_id]:
            return True
        if consulta_id == 4 and self.datos[client_id]["credits"][LINEAS] >= BATCH_CREDITS and self.termino_movies[client_id]:
            return True        
        return False


    def guardar_datos(self, consulta_id, datos, client_id):
        if not consulta_id in self.resultados_parciales[client_id]:
            self.resultados_parciales[client_id][consulta_id] = []
        self.resultados_parciales[client_id][consulta_id].append(create_dataframe(datos))


    def almacenar_csv(self, consulta, datos, client_id):
        if consulta == 3:
            csv = "ratings"
        else:
            csv = "credits"
        datos = create_dataframe(datos)
        self.datos[client_id][csv][LINEAS] += len(datos)
        self.datos[client_id][csv][DATOS].append(datos)

    def borrar_info(self, csv, client_id):
        self.datos[client_id][csv][DATOS] = []
        self.datos[client_id][csv][LINEAS] = 0

    def ejecutar_consulta(self, consulta_id, client_id):
        if client_id not in self.resultados_parciales:
            return False
        
        datos_cliente = self.resultados_parciales[client_id]
        if not datos_cliente:
            return False 
        
        datos = concat_data(datos_cliente[consulta_id])

        match consulta_id:
            case 3:
                return self.consulta_3(datos, client_id)
            case 4:
                return self.consulta_4(datos, client_id)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_3(self, datos, client_id):
        logging.info("Procesando datos para consulta 3")
        ratings = concat_data(self.datos[client_id]["ratings"][DATOS])
        self.borrar_info("ratings", client_id)
        if ratings.empty:
            return False
        ranking_arg_post_2000_df = datos[["id", "title"]].merge(ratings, on="id")
        return ranking_arg_post_2000_df

    def consulta_4(self, datos, client_id):
        logging.info(f"Procesando datos para consulta 4")
        credits = prepare_data_consult_4(self.datos[client_id]["credits"][DATOS])
        self.borrar_info("credits", client_id)
        if credits.empty:
            return False
        cast_arg_post_2000_df = datos[["id", "title"]].merge(credits, on="id")
        df_cast = cast_arg_post_2000_df.explode('cast')
        cast_and_movie_arg_post_2000_df = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})
        logging.info(f"lo que devuelve consulta 4 es {cast_and_movie_arg_post_2000_df}")
        return cast_and_movie_arg_post_2000_df


    def procesar_datos(self, consulta_id, tipo_mensaje, mensaje, client_id):
        """Procesa y guarda los datos dependiendo del tipo de mensaje."""
        contenido = obtener_body(mensaje)
        if client_id not in self.datos:
            self.crear_datos(client_id)
        if tipo_mensaje == "MOVIES":
            self.guardar_datos(consulta_id, contenido, client_id)
        else:
            self.almacenar_csv(consulta_id, contenido, client_id)


    def procesar_resultado(self, consulta_id, canal, destino, mensaje, enviar_func, client_id):
        """Envía el resultado si es posible."""
        if self.puede_enviar(consulta_id, client_id):
            resultado = self.ejecutar_consulta(consulta_id, client_id)
            enviar_func(canal, destino, resultado, mensaje, "RESULT")
    
    def enviar_eof(self, consulta_id, canal, destino, mensaje, enviar_func, client_id):
        if self.termino_movies[client_id] and (
            (consulta_id == 3 and self.datos[client_id]["ratings"][TERMINO]) or 
            (consulta_id == 4 and self.datos[client_id]["credits"][TERMINO])
        ):
            enviar_func(canal, destino, EOF, mensaje, EOF)
            self.termino_movies[client_id] = False



    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        try:
            if tipo_mensaje == "EOF":
                logging.info(f"Consulta {consulta_id} recibió EOF")
                self.termino_movies[client_id] = True
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            
            elif tipo_mensaje in ["MOVIES", "RATINGS", "CREDITS"]:
                self.procesar_datos(consulta_id, tipo_mensaje, mensaje, client_id)
                if tipo_mensaje != "MOVIES":
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)

            elif tipo_mensaje == "EOF_RATINGS":
                logging.info("Recibí todos los ratings")
                self.datos[client_id]["ratings"][TERMINO] = True
                if self.datos[client_id]["ratings"][LINEAS] != 0:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)

            elif tipo_mensaje == "EOF_CREDITS":
                logging.info("Recibí todos los credits")
                self.datos[client_id]["credits"][TERMINO] = True
                if self.datos[client_id]["credits"][LINEAS] != 0:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)

            mensaje['ack']() 
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

        

# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    consultas = os.getenv("CONSULTAS", "")
    worker_id = int(os.environ.get("WORKER_ID", 0))
    joiner = JoinerNode()
    iniciar_nodo(JOINER, joiner, consultas, worker_id)
