import sys
import logging
import os
import tempfile
import gc

import threading
from common.utils import EOF, concat_data, create_dataframe, get_batches
from common.utils import normalize_movies_df, normalize_credits_df, normalize_ratings_df
from common.communication import iniciar_nodo, obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje
import pandas as pd # type: ignore

JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2

BATCH_CREDITS, BATCH_RATINGS = get_batches(JOINER)

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.termino_movies = {}
        self.datos = {}

        self.locks = {}
        self.files_on_disk = {}
        self.file_paths = {}

    def eliminar(self):
        self.resultados_parciales = {}
        self.termino_movies = {}
        self.datos = {}

    def crear_datos(self, client_id):
        self.datos[client_id] = {"ratings": [[], 0, False], "credits": [[], 0, False]}
         # "CSV": (datos, cantidad de datos, recibio el EOF correspondiente)
        self.termino_movies[client_id] = False
        self.resultados_parciales[client_id] = {}

        self.locks[client_id] = {
            "ratings": threading.Lock(),
            "credits": threading.Lock(),
        }
        self.files_on_disk[client_id] = {
            "ratings": False,
            "credits": False,
        }
        self.file_paths[client_id] = {
            "ratings": tempfile.NamedTemporaryFile(delete=False).name,
            "credits": tempfile.NamedTemporaryFile(delete=False).name,
        }

    def puede_enviar(self, consulta_id, client_id):
        if not self.termino_movies[client_id]:
            return False
        if consulta_id == 3:
            return (self.datos[client_id]["ratings"][LINEAS] >= BATCH_RATINGS) or self.files_on_disk[client_id]["ratings"]
        if consulta_id == 4:
            return (self.datos[client_id]["credits"][LINEAS] >= BATCH_CREDITS) or self.files_on_disk[client_id]["credits"]
        return False

    def guardar_datos(self, consulta_id, datos, client_id):
        if consulta_id not in self.resultados_parciales[client_id]:
            self.resultados_parciales[client_id][consulta_id] = []

        df = create_dataframe(datos)
        df = normalize_movies_df(df)
        self.resultados_parciales[client_id][consulta_id].append(df)

    def almacenar_csv(self, consulta, datos, client_id):
        csv = "ratings" if consulta == 3 else "credits"
        df = create_dataframe(datos)

        if csv == "ratings":
            df = normalize_ratings_df(df)
        elif csv == "credits":
            df = normalize_credits_df(df)

        self.datos[client_id][csv][LINEAS] += len(df)
        self.datos[client_id][csv][DATOS].append(df)

        if not self.termino_movies[client_id]:
            bsize = BATCH_RATINGS if csv == "ratings" else BATCH_CREDITS
            if self.datos[client_id][csv][LINEAS] >= bsize:
                with self.locks[client_id][csv]:
                    df_total = concat_data(self.datos[client_id][csv][DATOS])
                    df_total.to_csv(
                        self.file_paths[client_id][csv],
                        mode='a',
                        header=not self.files_on_disk[client_id][csv],
                        index=False
                    )
                    self.files_on_disk[client_id][csv] = True
                self.borrar_info(csv, client_id)

    def leer_batches_de_disco(self, client_id, csv, batch_size):
        with self.locks[client_id][csv]:
            file_path = self.file_paths[client_id][csv]
            if not os.path.exists(file_path):
                logging.info(f"Filepath doesn't exist {file_path}")
                return

            for batch in pd.read_csv(file_path, chunksize=batch_size):
                if csv == "ratings":
                    batch = normalize_ratings_df(batch)
                elif csv == "credits":
                    batch = normalize_credits_df(batch)
                yield batch

    def enviar_resultados_credits_disco(self, datos, client_id, canal, destino, mensaje, enviar_func):
        for batch in self.leer_batches_de_disco(client_id, "credits", BATCH_CREDITS):
            self.procesar_y_enviar_batch_credit(batch, datos, canal, destino, mensaje, enviar_func)
            del batch
            gc.collect()
        try:
            os.remove(self.file_paths[client_id]["credits"])
        except Exception as e:
            logging.error(f"No se pudo borrar el archivo temporal credits: {e}")

        self.files_on_disk[client_id]["credits"] = False

    def enviar_resultados_ratings_disco(self, datos, client_id, canal, destino, mensaje, enviar_func):
        for batch in self.leer_batches_de_disco(client_id, "ratings", BATCH_RATINGS):
            self.procesar_y_enviar_batch_ratings(batch, datos, canal, destino, mensaje, enviar_func)
            del batch
            gc.collect()
        try:
            os.remove(self.file_paths[client_id]["ratings"])
        except Exception as e:
            logging.error(f"No se pudo borrar el archivo temporal ratings: {e}")
        self.files_on_disk[client_id]["ratings"] = False

    def procesar_y_enviar_batch_credit(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is not None and not batch.empty:
            batch = normalize_credits_df(batch)
            df_merge = datos[["id", "title"]].merge(batch, on="id", how="inner")
            df_merge = df_merge[df_merge['cast'].map(lambda x: len(x) > 0)]
            if not df_merge.empty:
                df_cast = df_merge.explode('cast')
                result = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})
                enviar_func(canal, destino, result, mensaje, "RESULT")

    def procesar_y_enviar_batch_ratings(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is not None and not batch.empty:
            batch = normalize_ratings_df(batch)
            df_merge = datos[["id", "title"]].merge(batch, on="id", how="inner")
            if not df_merge.empty:
                enviar_func(canal, destino, df_merge, mensaje, "RESULT")

    def borrar_info(self, csv, client_id):
        self.datos[client_id][csv][DATOS] = []
        self.datos[client_id][csv][LINEAS] = 0

    def ejecutar_consulta(self, datos, consulta_id, client_id):
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
        resultados = []

        ratings = concat_data(self.datos[client_id]["ratings"][DATOS])
        if not ratings.empty:
            ratings = normalize_ratings_df(ratings)
            df_merge = datos[["id", "title"]].merge(ratings, on="id", how="inner")
            resultados.append(df_merge)

        self.borrar_info("ratings", client_id)

        if resultados:
            return pd.concat(resultados, ignore_index=True)

        return False

    def consulta_4(self, datos, client_id):
        logging.info("Procesando datos para consulta 4") 
        resultados = []

        credits = concat_data(self.datos[client_id]["credits"][DATOS])
        if not credits.empty:
            credits = normalize_credits_df(credits)
            df_merge = datos[["id", "title"]].merge(credits, on="id", how="inner")
            df_merge = df_merge[df_merge['cast'].map(lambda x: len(x) > 0)]
            if not df_merge.empty:
                df_cast = df_merge.explode('cast')
                result = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})
                resultados.append(result)

        self.borrar_info("credits", client_id)

        if resultados:
            return pd.concat(resultados, ignore_index=True)

        return False

    def procesar_datos(self, consulta_id, tipo_mensaje, mensaje, client_id):
        contenido = obtener_body(mensaje)
        if client_id not in self.datos:
            self.crear_datos(client_id)
        if tipo_mensaje == "MOVIES":
            self.guardar_datos(consulta_id, contenido, client_id)
        else:
            self.almacenar_csv(consulta_id, contenido, client_id)

    def procesar_resultado(self, consulta_id, canal, destino, mensaje, enviar_func, client_id):
        if self.puede_enviar(consulta_id, client_id):
            if client_id not in self.resultados_parciales:
                return False
            datos_cliente = self.resultados_parciales[client_id]
            if not datos_cliente or consulta_id not in datos_cliente:
                return False
            datos = concat_data(datos_cliente[consulta_id])
            datos = normalize_movies_df(datos)

            resultado = self.ejecutar_consulta(datos, consulta_id, client_id)
            enviar_func(canal, destino, resultado, mensaje, "RESULT")

            if consulta_id == 3 and self.files_on_disk[client_id]["ratings"]:
              self.enviar_resultados_ratings_disco(datos, client_id, canal, destino, mensaje, enviar_func)
            elif consulta_id == 4 and self.files_on_disk[client_id]["credits"]:
              self.enviar_resultados_credits_disco(datos, client_id, canal, destino, mensaje, enviar_func)


    def enviar_eof(self, consulta_id, canal, destino, mensaje, enviar_func, client_id):
        if self.termino_movies[client_id] and (
            (consulta_id == 3 and (self.datos[client_id]["ratings"][TERMINO] or self.files_on_disk[client_id]["ratings"]))
            or (consulta_id == 4 and (self.datos[client_id]["credits"][TERMINO] or self.files_on_disk[client_id]["credits"]))
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
                if self.datos[client_id]["ratings"][LINEAS] != 0 or self.files_on_disk[client_id]["ratings"]:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            elif tipo_mensaje == "EOF_CREDITS":
                logging.info("Recibí todos los credits")
                self.datos[client_id]["credits"][TERMINO] = True
                if self.datos[client_id]["credits"][LINEAS] != 0 or self.files_on_disk[client_id]["credits"]:
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