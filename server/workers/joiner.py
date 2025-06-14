from multiprocessing import Process
import logging
import os
import tempfile
import threading
from common.utils import EOF, borrar_contenido_carpeta, concat_data, create_dataframe, fue_reiniciado, get_batches, obtiene_nombre_contenedor, prepare_data_consult_4
from common.utils import normalize_movies_df, normalize_credits_df, normalize_ratings_df
from common.communication import iniciar_nodo, obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
import pandas as pd # type: ignore
from common.excepciones import ConsultaInexistente


JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2
TMP_DIR = "/tmp/joiner_tmp"

BATCH_CREDITS, BATCH_RATINGS = get_batches(JOINER)

HEALTH_FILE = f"/app/reinicio_flags/{obtiene_nombre_contenedor(JOINER)}.data"
MOVIES_FILE = f"/app/reinicio_flags/{obtiene_nombre_contenedor(JOINER)}-movies.data"
DATA_FILE = f"/app/reinicio_flags/{obtiene_nombre_contenedor(JOINER)}-data.data"
MOD_DATOS = "datos"
MOD_TERM = "termino_movies"
MOD_RESULT = "resultados_parciales"
MOD_DISK = "files_on_disk"
MOD_PATHS = "file_paths"

import ast  # más seguro que eval

def parse_linea_repr(linea_str):
    return ast.literal_eval(linea_str.strip())

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:

    def __init__(self, reiniciado=None):
        self.resultados_parciales = {}
        self.result_par_health = []
        self.termino_movies = {}
        self.datos = {}
        self.datos_par_health = []
        self.locks = {}
        self.files_on_disk = {}
        self.file_paths = {}
        self.cambios = {}
        if reiniciado:
            self.cargar_estado()
            print(f"a ver  {self.datos}", flush=True)
            print(f"a ver  {self.resultados_parciales}", flush=True)
        self.archivo_result = open(MOVIES_FILE, "ab")
        self.archivo_datos = open(DATA_FILE, "ab")
        self.actualizaciones = {
            MOD_DATOS: self.datos_par_health,
            MOD_TERM: self.termino_movies,
            MOD_RESULT: self.result_par_health,
            MOD_DISK: self.files_on_disk,
            MOD_PATHS: self.file_paths,
        }

    def eliminar(self, es_global):
        if es_global:
            try:
                borrar_contenido_carpeta()
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
        self.result_par_health.clear()
        self.datos_par_health.clear()
        self.termino_movies.clear()
        self.datos.clear()
        self.locks.clear()
        self.files_on_disk.clear()
        self.file_paths.clear()
        self.cambios.clear()

    def modificar(self, claves):
        for clave in claves:
            self.cambios[clave] = True


    def cargar_estado(self):
        # Inicializar estructuras si no existen
        self.resultados_parciales = {}
        self.datos = {}

        # Leer y reconstruir DATOS
        try:
            with open(DATA_FILE, "rb") as f_datos:
                for linea in f_datos:
                    print("Entra en el archivo data")
                    lineas_parsed = parse_linea_repr(linea.decode("utf-8"))
                    for parsed in lineas_parsed:
                        if len(parsed) != 4:
                            continue
                        client_id, csv, datos, cantidad = parsed

                        if client_id not in self.datos:
                            self.datos[client_id] = {}
                        if csv not in self.datos[client_id]:
                            self.datos[client_id][csv] = [[], 0, False]

                        self.datos[client_id][csv][0].append(datos)
                        self.datos[client_id][csv][1] = cantidad  # se pisa siempre
            self.cambios[MOD_DATOS] = False

        except Exception as e:
            print(f"Error cargando datos_par_health: {e}", flush=True)

        # Leer y reconstruir RESULTADOS PARCIALES
        try:
            with open(MOVIES_FILE, "rb") as f_result:
                for linea in f_result:
                    print("Entra en el archivo movies")
                    lineas_parsed = parse_linea_repr(linea.decode("utf-8"))
                    for parsed in lineas_parsed:
                        if len(parsed) != 3:
                            continue
                        client_id, consulta_id, datos = parsed

                        if client_id not in self.resultados_parciales:
                            self.resultados_parciales[client_id] = {}
                        if consulta_id not in self.resultados_parciales[client_id]:
                            self.resultados_parciales[client_id][consulta_id] = []

                        self.resultados_parciales[client_id][consulta_id].append(datos)
            self.cambios[MOD_RESULT] = False

        except Exception as e:
            print(f"Error cargando result_par_health: {e}", flush=True)

        # Leer el HEALTH_FILE para claves como termino_movies, files_on_disk, etc.
        try:
            with open(HEALTH_FILE, "rb") as f:
                contenido = f.read()
                offset = 0
                while offset < len(contenido):
                    len_clave = int.from_bytes(contenido[offset:offset+4], "big")
                    offset += 4
                    clave = contenido[offset:offset+len_clave].decode("utf-8")
                    offset += len_clave

                    len_valor = int.from_bytes(contenido[offset:offset+4], "big")
                    offset += 4
                    valor_raw = contenido[offset:offset+len_valor].decode("utf-8")
                    offset += len_valor

                    valor = ast.literal_eval(valor_raw)  # más seguro que eval

                    if clave == MOD_TERM:
                        self.termino_movies = valor
                    elif clave == MOD_DISK:
                        self.files_on_disk = valor
                    elif clave == MOD_PATHS:
                        self.file_paths = valor

        except FileNotFoundError:
            print("No se encontró HEALTH_FILE", flush=True)
        except Exception as e:
            print(f"Error al cargar estado de HEALTH_FILE: {e}", flush=True)


    def guardar_estado(self):
        try:
        
            if self.cambios.get(MOD_DATOS, False):
                self.guardar_estado_csv(MOD_DATOS)
            if self.cambios.get(MOD_RESULT, False):
                self.guardar_estado_csv(MOD_RESULT)

            otras_claves = [k for k in self.cambios if k not in (MOD_DATOS, MOD_RESULT)]
            cambios_otras = any(self.cambios.get(k, False) for k in otras_claves)

            if cambios_otras:
                with open(HEALTH_FILE, "wb") as f:
                    buffer = bytearray()
                    for clave in otras_claves:
                        if self.cambios.get(clave, False):
                            valor = self.actualizaciones[clave]
                            clave_b = clave.encode("utf-8")
                            valor_b = str(valor).encode("utf-8")
                            buffer.extend(len(clave_b).to_bytes(4, "big"))
                            buffer.extend(clave_b)
                            buffer.extend(len(valor_b).to_bytes(4, "big"))
                            buffer.extend(valor_b)
                            self.cambios[clave] = False
                    f.write(buffer)
        except Exception as e:
            print(f"Error al guardar estado: {e}", flush=True)


    def guardar_estado_csv(self, csv):
        archivo = self.archivo_result if csv == MOD_RESULT else self.archivo_datos
        valor = self.actualizaciones[csv]
        archivo.write((repr(valor) + '\n').encode('utf-8'))
        if csv == MOD_RESULT:
            self.result_par_health.clear()
        if csv == MOD_DATOS:
            self.datos_par_health.clear()
        self.cambios[csv] = False


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
        df = normalize_movies_df(df)
        self.resultados_parciales[client_id][consulta_id].append(df)
        self.result_par_health = [client_id, consulta_id, datos]

    def almacenar_csv(self, consulta_id, datos, client_id):
        csv = "ratings" if consulta_id == 3 else "credits"
        df = create_dataframe(datos)
        if csv == "ratings":
            df = normalize_ratings_df(df)
        elif csv == "credits":
            df = normalize_credits_df(df)

        self.datos[client_id][csv][LINEAS] += len(df)
        self.datos[client_id][csv][DATOS].append(df)
        self.datos_par_health = [client_id, csv, datos, self.datos[client_id][csv][LINEAS]]

        if not self.termino_movies[client_id][consulta_id]:
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

    def leer_batches_de_disco(self, client_id, csv):
        batch_size = BATCH_RATINGS if csv == "ratings" else BATCH_CREDITS
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

    def enviar_resultados_disco(self, datos, client_id, canal, destino, mensaje, enviar_func, consulta_id):
        csv = "ratings" if consulta_id == 3 else "credits"
        for batch in self.leer_batches_de_disco(client_id, csv):
            if consulta_id == 3:
                self.procesar_y_enviar_batch_ratings(batch, datos, canal, destino, mensaje, enviar_func)
            else:
                self.procesar_y_enviar_batch_credit(batch, datos, canal, destino, mensaje, enviar_func)
            batch = None
        try:
            os.remove(self.file_paths[client_id][csv])
        except Exception as e:
            logging.error(f"No se pudo borrar el archivo temporal {csv}: {e}")
        self.files_on_disk[client_id][csv] = False


    def procesar_y_enviar_batch_credit(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is not None and not batch.empty:
            df_merge = datos[["id", "title"]].merge(batch, on="id", how="inner")
            df_merge = df_merge[df_merge['cast'].map(lambda x: len(x) > 0)]
            if not df_merge.empty:
                df_cast = df_merge.explode('cast')
                result = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})
                enviar_func(canal, destino, result, mensaje, "RESULT")

    def procesar_y_enviar_batch_ratings(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is not None and not batch.empty:
            df_merge = datos[["id", "title"]].merge(batch, on="id", how="inner")
            if not df_merge.empty:
                enviar_func(canal, destino, df_merge, mensaje, "RESULT")

    def borrar_info(self, csv, client_id):
        self.datos[client_id][csv][DATOS] = []
        self.datos[client_id][csv][LINEAS] = 0
        self.modificar([MOD_DATOS])

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
        logging.info(f"Procesando datos para consulta 4")
        credits = prepare_data_consult_4(self.datos[client_id]["credits"][DATOS])
        self.borrar_info("credits", client_id)
        if credits.empty:
            return False
        cast_arg_post_2000_df = datos[["id", "title"]].merge(credits, on="id")
        df_cast = cast_arg_post_2000_df.explode('cast')
        cast_and_movie_arg_post_2000_df = df_cast[['id', 'cast']].rename(columns={'cast': 'name'})
        return cast_and_movie_arg_post_2000_df

    def procesar_datos(self, consulta_id, tipo_mensaje, mensaje, client_id):
        contenido = obtener_body(mensaje)
        if client_id not in self.datos:
            self.crear_datos(client_id)
            self.modificar([MOD_DATOS, MOD_TERM, MOD_RESULT, MOD_PATHS, MOD_DISK])
        if tipo_mensaje == "MOVIES":
            self.guardar_datos(consulta_id, contenido, client_id)
            self.modificar([MOD_RESULT])
        else:
            self.almacenar_csv(consulta_id, contenido, client_id)
            self.modificar([MOD_DATOS, MOD_DISK])


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
                self.modificar([MOD_RESULT])
        self.modificar([MOD_PATHS, MOD_DISK])



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
        try:
            if tipo_mensaje == "EOF":
                logging.info(f"Se recibio todo Movies, consulta {consulta_id} recibió EOF")
                self.termino_movies[client_id][consulta_id] = True
                self.modificar([MOD_TERM])
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            elif tipo_mensaje in ["MOVIES", "RATINGS", "CREDITS"]:
                self.procesar_datos(consulta_id, tipo_mensaje, mensaje, client_id)
                if tipo_mensaje != "MOVIES":
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            elif tipo_mensaje == "EOF_RATINGS":
                logging.info("Recibí todos los ratings")
                self.datos[client_id]["ratings"][TERMINO] = True
                self.modificar([MOD_DATOS])
                if self.datos[client_id]["ratings"][LINEAS] != 0 or self.files_on_disk[client_id]["ratings"]:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            elif tipo_mensaje == "EOF_CREDITS":
                logging.info("Recibí todos los credits")
                self.datos[client_id]["credits"][TERMINO] = True
                self.modificar([MOD_DATOS])
                if self.datos[client_id]["credits"][LINEAS] != 0 or self.files_on_disk[client_id]["credits"]:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id)
            self.guardar_estado()
            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")

# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    run(JOINER, JoinerNode)

