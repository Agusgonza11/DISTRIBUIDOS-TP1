import csv
import pickle
import logging
import os
import tempfile
import threading
from common.utils import EOF, borrar_contenido_carpeta, concat_data, create_dataframe, get_batches, obtiene_nombre_contenedor, prepare_data_consult_4, write_dicts_to_csv
from common.utils import normalize_movies_df, normalize_credits_df, normalize_ratings_df
from common.communication import obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
import pandas as pd # type: ignore
from common.excepciones import ConsultaInexistente


JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2
TMP_DIR = f"/tmp/{obtiene_nombre_contenedor(JOINER)}_tmp"

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
        self.cambios = {}
        self.modifico = False
        self.health_file = f"{TMP_DIR}/health_file.data"
        self.cargar_estado()

    def eliminar(self, es_global):
        if es_global:
            try:
                borrar_contenido_carpeta(self.health_file)
                for client_id, paths in self.file_paths.items():
                    for tipo, path in paths.items():
                        try:
                            if os.path.exists(path):
                                os.remove(path)
                                print(f"Archivo borrado: {path}")
                        except Exception as e:
                            print(f"Error borrando {path}: {e}")
                logging.info(f"Volumen limpiado por shutdown global")
            except Exception as e:
                logging.error(f"Error limpiando volumen en shutdown global: {e}")
        
        self.resultados_parciales.clear()
        self.termino_movies.clear()
        self.datos.clear()
        self.locks.clear()
        self.files_on_disk.clear()
        self.file_paths.clear()
        self.cambios.clear()

    def marcar_modificado(self, clave, valor):
        try:
            self.cambios[clave] = valor
            self.modifico = True
        except Exception as e:
            print(f"Error al serializar '{clave}': {e}", flush=True)


    def cargar_estado(self):
        try:
            with open(self.health_file, "rb") as f:
                size_bytes = f.read(4)
                if len(size_bytes) < 4:
                    logging.warning("Archivo de estado corrupto o vacío (<4 bytes)")
                    return

                size = int.from_bytes(size_bytes, byteorder='big')
                state_bytes = f.read(size)
                if len(state_bytes) < size:
                    logging.warning("Archivo de estado incompleto/corrupto (no hay enough bytes para pickle)")
                    return
                snap = pickle.loads(state_bytes)
            self.datos = snap.get("datos", {})
            self.termino_movies = snap.get("termino_movies", {})
            self.resultados_parciales = snap.get("resultados_parciales", {})
            self.files_on_disk = snap.get("files_on_disk", {})
            self.file_paths = snap.get("file_paths", {})
            self.locks = {
                cid: {
                    "ratings": threading.Lock(),
                    "credits": threading.Lock()
                } for cid in self.datos
            }
            
            logging.info("Estado cargado")
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)



    def guardar_estado(self):
        if not self.modifico:
            return
        try:
            snap = {
                "datos": self.datos,
                "termino_movies": self.termino_movies,
                "resultados_parciales": self.resultados_parciales,
                "files_on_disk": self.files_on_disk,
                "file_paths": self.file_paths
            }
            with open(self.health_file, "wb") as f:
                pickled = pickle.dumps(snap)
                f.write(len(pickled).to_bytes(4, "big"))
                f.write(pickled)
            self.modifico = False
        except Exception as e:
            print(f"Error al guardar estado: {e}", flush=True)


    def crear_datos(self, client_id):
        self.datos[client_id] = {"ratings": [[], 0, False], "credits": [[], 0, False]}
         # "CSV": (datos, cantidad de datos, recibio el EOF correspondiente)
        self.termino_movies[client_id] = {
            3: False,
            4: False,
        }
        self.resultados_parciales[client_id] = {
            3: [],
            4: [],
        }

        self.locks[client_id] = {
            "ratings": threading.Lock(),
            "credits": threading.Lock(),
        }
        self.files_on_disk[client_id] = {
            "ratings": False,
            "credits": False,
        }

        os.makedirs(TMP_DIR, exist_ok=True)

        self.file_paths[client_id] = {
            "ratings": tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR).name,
            "credits": tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR).name,
        }

    def puede_enviar(self, consulta_id, client_id):
        if not self.termino_movies[client_id][consulta_id]:
            return False
        if consulta_id == 3:
            return (self.datos[client_id]["ratings"][LINEAS] >= BATCH_RATINGS) or self.files_on_disk[client_id]["ratings"]
        if consulta_id == 4:
            return (self.datos[client_id]["credits"][LINEAS] >= BATCH_CREDITS) or self.files_on_disk[client_id]["credits"]

        return False

    def guardar_datos(self, consulta_id, datos, client_id):
        df = create_dataframe(datos)
        #df = normalize_movies_df(df)
        #print(f"los datos movies son {datos}", flush=True)
        self.resultados_parciales[client_id][consulta_id].append(df)

    def almacenar_csv(self, consulta_id, datos, client_id):
        csv = "ratings" if consulta_id == 3 else "credits"
        df = create_dataframe(datos)
        #print(f"los datos csv son {datos}", flush=True)
        #if csv == "ratings":
        #    df = normalize_ratings_df(df)
        #elif csv == "credits":
        #    df = normalize_credits_df(df)

        self.datos[client_id][csv][LINEAS] += len(df)
        self.datos[client_id][csv][DATOS].append(df)

        if not self.termino_movies[client_id][consulta_id]:
            bsize = BATCH_RATINGS if csv == "ratings" else BATCH_CREDITS
            if self.datos[client_id][csv][LINEAS] >= bsize:
                with self.locks[client_id][csv]:
                    # concat_data junta listas de listas: convertimos a lista simple
                    df_total = []
                    for chunk in self.datos[client_id][csv][DATOS]:
                        df_total.extend(chunk)
                    write_dicts_to_csv(
                        self.file_paths[client_id][csv],
                        df_total,
                        append=self.files_on_disk[client_id][csv]
                    )
                    self.files_on_disk[client_id][csv] = True
                self.borrar_info(csv, client_id)


    def leer_batches_de_disco(self, client_id, csv_name):
        batch_size = BATCH_RATINGS if csv_name == "ratings" else BATCH_CREDITS
        file_path = self.file_paths[client_id][csv_name]

        with self.locks[client_id][csv_name]:
            if not os.path.exists(file_path):
                logging.info(f"Filepath doesn't exist {file_path}")
                return

            with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                batch = []
                for row in reader:
                    batch.append(row)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:
                    yield batch


    def enviar_resultados_disco(self, datos, client_id, canal, destino, mensaje, enviar_func, consulta_id):
        csv = "ratings" if consulta_id == 3 else "credits"
        for batch in self.leer_batches_de_disco(client_id, csv):
            if consulta_id == 3:
                self.procesar_y_enviar_batch_ratings(batch, datos, canal, destino, mensaje, enviar_func)
            else:
                self.procesar_y_enviar_batch_credit(batch, datos, canal, destino, mensaje, enviar_func)
            batch = None
            # logging.info("Enviando datos de disco...\n")

        try:
            os.remove(self.file_paths[client_id][csv])
        except Exception as e:
            logging.error(f"No se pudo borrar el archivo temporal {csv}: {e}")
        self.files_on_disk[client_id][csv] = False


    def procesar_y_enviar_batch_credit(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is None or len(batch) == 0:
            return
        datos_dict = {entry['id']: entry['title'] for entry in datos}
        joined = []
        for row in batch:
            movie_id = row.get('id')
            cast_list = row.get('cast', [])
            if movie_id in datos_dict and isinstance(cast_list, list) and len(cast_list) > 0:
                for cast_member in cast_list:
                    joined.append({'id': movie_id, 'name': cast_member})
        print(f"en procesar y enviar 4 {joined}", flush=True)

        if len(joined) > 0:
            enviar_func(canal, destino, joined, mensaje, "RESULT")


    def procesar_y_enviar_batch_ratings(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is not None and not batch.empty:
            df_merge = datos[["id", "title"]].merge(batch, on="id", how="inner")
            if not df_merge.empty:
                enviar_func(canal, destino, df_merge, mensaje, "RESULT")

    def borrar_info(self, csv, client_id):
        self.datos[client_id][csv][DATOS] = []
        self.datos[client_id][csv][LINEAS] = 0
        self.marcar_modificado("datos", self.datos)

    def limpiar_consulta(self, client_id, consulta_id):
        csv = "ratings" if consulta_id == 3 else "credits"
        path = self.file_paths[client_id][csv]
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as ex:
                logging.error(f"No se pudo borrar ratings temp: {ex}")
        self.files_on_disk[client_id][csv] = False
        self.file_paths[client_id][csv] = ""
        self.borrar_info(csv, client_id)
        self.resultados_parciales[client_id][consulta_id] = []

    def ejecutar_consulta(self, datos, consulta_id, client_id):
        match consulta_id:
            case 3:
                return self.consulta_3(datos, client_id)
            case 4:
                return self.consulta_4(datos, client_id)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                raise ConsultaInexistente(f"Consulta {consulta_id} no encontrada")

    def consulta_3(self, datos, client_id):
        logging.info("Procesando datos para consulta 3") 
        ratings = concat_data(self.datos[client_id]["ratings"][DATOS])
        self.borrar_info("ratings", client_id)
        if ratings.empty:
            return False
        ranking_arg_post_2000_df = datos[["id", "title"]].merge(ratings, on="id")
        return ranking_arg_post_2000_df

    def consulta_4(self, datos, client_id):
        logging.info("Procesando datos para consulta 4")
        credits = prepare_data_consult_4(self.datos[client_id]["credits"][DATOS])
        self.borrar_info("credits", client_id)
        if not credits:
            return False
        datos_list = [ {"id": d["id"], "title": d["title"]} for d in datos ]
        merged = []
        credits_by_id = {}
        for c in credits:
            credits_by_id.setdefault(c["id"], []).append(c)
        for movie in datos_list:
            movie_id = movie["id"]
            if movie_id in credits_by_id:
                for credit in credits_by_id[movie_id]:
                    merged.append({**movie, **credit})
        exploded = []
        for entry in merged:
            cast_list = entry.get('cast', [])
            for cast_member in cast_list:
                exploded.append({
                    "id": entry["id"],
                    "name": cast_member 
                })
        print(f"en consulta 4 {exploded}", flush=True)
        return exploded



    def procesar_datos(self, consulta_id, tipo_mensaje, mensaje, client_id):
        contenido = obtener_body(mensaje)
        if client_id not in self.datos:
            self.crear_datos(client_id)
            self.marcar_modificado("datos", self.datos)
            self.marcar_modificado("termino_movies", self.termino_movies)
            self.marcar_modificado("resultados_parciales", self.resultados_parciales)
            self.marcar_modificado("files_on_disk", self.files_on_disk)
            self.marcar_modificado("file_paths", self.file_paths)
        if tipo_mensaje == "MOVIES":
            self.guardar_datos(consulta_id, contenido, client_id)
            self.marcar_modificado("resultados_parciales", self.resultados_parciales)
        else:
            self.almacenar_csv(consulta_id, contenido, client_id)
            self.marcar_modificado("datos", self.datos)
            self.marcar_modificado("files_on_disk", self.files_on_disk)

    def procesar_resultado(self, consulta_id, canal, destino, mensaje, enviar_func, client_id):
        if self.puede_enviar(consulta_id, client_id):
            if client_id not in self.resultados_parciales:
                logging.info(f"Para el cliente {client_id} NO esta en resultados parciales")
                return False
            datos_cliente = self.resultados_parciales[client_id]
            if not datos_cliente or consulta_id not in datos_cliente:
                logging.info(f"Para la consulta {consulta_id} NO esta en resultados parciales[{client_id}]")
                return False
            datos = concat_data(datos_cliente[consulta_id])
            resultado = self.ejecutar_consulta(datos, consulta_id, client_id)
            enviar_func(canal, destino, resultado, mensaje, "RESULT")

            if consulta_id == 3 and self.files_on_disk[client_id]["ratings"]:
                self.enviar_resultados_disco(datos, client_id, canal, destino, mensaje, enviar_func, consulta_id)
            elif consulta_id == 4 and self.files_on_disk[client_id]["credits"]:
                self.enviar_resultados_disco(datos, client_id, canal, destino, mensaje, enviar_func, consulta_id)

        if self.termino_movies[client_id][consulta_id]:
            if self.datos[client_id]["ratings"][TERMINO] or self.datos[client_id]["credits"][TERMINO]:
                self.limpiar_consulta(client_id, consulta_id)
                self.marcar_modificado("resultados_parciales", self.resultados_parciales)
        self.marcar_modificado("file_paths", self.file_paths)
        self.marcar_modificado("files_on_disk", self.files_on_disk)

    def enviar_eof(self, consulta_id, canal, destino, mensaje, enviar_func, client_id):
        if self.termino_movies[client_id][consulta_id] and (
            (consulta_id == 3 and (self.datos[client_id]["ratings"][TERMINO]))
            or (consulta_id == 4 and (self.datos[client_id]["credits"][TERMINO]))
        ):
            enviar_func(canal, destino, EOF, mensaje, EOF)

    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        self.modifico = False
        if tipo_mensaje == "EOF":
            logging.info(f"Se recibio todo Movies, consulta {consulta_id} recibió EOF")
            self.termino_movies[client_id][consulta_id] = True
            self.marcar_modificado("termino_movies", self.termino_movies)
            self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
        elif tipo_mensaje in ["MOVIES", "RATINGS", "CREDITS"]:
            self.procesar_datos(consulta_id, tipo_mensaje, mensaje, client_id)
            if tipo_mensaje != "MOVIES":
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
        elif tipo_mensaje == "EOF_RATINGS":
            logging.info("Recibí todos los ratings")
            self.datos[client_id]["ratings"][TERMINO] = True
            self.marcar_modificado("datos", self.datos)
            if self.datos[client_id]["ratings"][LINEAS] != 0 or self.files_on_disk[client_id]["ratings"]:
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
        elif tipo_mensaje == "EOF_CREDITS":
            logging.info("Recibí todos los credits")
            self.datos[client_id]["credits"][TERMINO] = True
            self.marcar_modificado("datos", self.datos)
            if self.datos[client_id]["credits"][LINEAS] != 0 or self.files_on_disk[client_id]["credits"]:
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            #self.guardar_estado()
        mensaje['ack']()


# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    run(JOINER, JoinerNode)

