import ast
import csv
import logging
import os
import tempfile
import threading
from common.utils import EOF, concat_data, create_dataframe, get_batches, obtener_batch, obtiene_nombre_contenedor, parse_credits, parse_datos, prepare_data_consult_4, write_dicts_to_csv
from common.communication import obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from common.excepciones import ConsultaInexistente, ErrorCargaDelEstado
from common.transaction import ACCION, Transaction


JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2
TMP_DIR = f"/tmp/{obtiene_nombre_contenedor(JOINER)}_tmp"

BATCH_CREDITS, BATCH_RATINGS = get_batches(JOINER)

DATA = "datos"
DATA_TERM = "termino_datos"
TERM = "termino_movies"
RESULT = "resultados_parciales"
DISK = "files_on_disk"
PATH = "file_paths"

NO_ENVIAR = "no_enviar"
ENVIAR = "enviar"

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
        self.transaction = Transaction(TMP_DIR)
        if self.transaction.cargar_estado(self):
            for client_id in self.resultados_parciales.keys():
                print(f"entra a actualizar {self.file_paths[client_id]['ratings']}", flush=True)
                self.transaction.commit(PATH, [client_id, self.file_paths[client_id]["ratings"], self.file_paths[client_id]["credits"]])



    def reconstruir(self, clave, contenido):
        partes = contenido.split("|")
        if len(partes) < 2:
            raise ErrorCargaDelEstado(f"Contenido malformado: {contenido!r}")
        client_id, *contenido = partes
        if client_id not in self.resultados_parciales:
            self.crear_datos(client_id, True)
        match clave:
            case "datos":
                csv_almacenado, datos, lineas = contenido
                if datos == "BORRADO":
                    self.datos[client_id][csv_almacenado][DATOS] = []
                    self.datos[client_id][csv_almacenado][LINEAS] = 0
                else:
                    if csv_almacenado == "ratings":
                        valor = parse_datos(datos)
                    elif csv_almacenado == "credits":
                        valor = parse_credits(datos)
                    self.datos[client_id][csv_almacenado][DATOS].append(valor)
                    self.datos[client_id][csv_almacenado][LINEAS] = int(lineas)
            case "termino_datos":
                csv_almacenado, booleano = contenido
                valor = booleano == "True"
                self.datos[client_id][csv_almacenado][TERMINO] = valor 
            case "termino_movies":
                consulta_id, booleano = contenido
                consulta_id = int(consulta_id)
                valor = booleano == "True"
                self.termino_movies[client_id][consulta_id] = valor
            case "resultados_parciales":
                consulta_id, valor = contenido
                consulta_id = int(consulta_id)
                if valor == "BORRADO":
                    self.resultados_parciales[client_id][consulta_id] = []
                else:
                    self.resultados_parciales[client_id][consulta_id].append(parse_datos(valor))
            case "files_on_disk":
                csv_almacenado, booleano = contenido
                valor = booleano == "True"
                self.files_on_disk[client_id][csv_almacenado] = valor 
            case "file_paths":
                ratings, credits = contenido
                if not client_id in self.file_paths:
                    self.file_paths[client_id] = {"ratings": None, "credits": None}
                self.file_paths[client_id]["ratings"] = ratings
                self.file_paths[client_id]["credits"] = credits
            case _:
                raise ErrorCargaDelEstado(f"Error en la carga del estado")


    def eliminar(self, es_global):
        if es_global:
            try:
                self.transaction.borrar_carpeta()
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



    def crear_datos(self, client_id, reiniciado=None):
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
        if reiniciado == True: return
        self.file_paths[client_id] = {
            "ratings": tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR).name,
            "credits": tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR).name,
        }
        self.transaction.commit(PATH, [client_id, self.file_paths[client_id]["ratings"], self.file_paths[client_id]["credits"]])


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
        self.resultados_parciales[client_id][consulta_id].append(df)
        self.transaction.commit(RESULT, [client_id, consulta_id, df])


    def almacenar_csv(self, consulta_id, datos, client_id):
        csv = "ratings" if consulta_id == 3 else "credits"
        df = create_dataframe(datos)
        self.datos[client_id][csv][LINEAS] += len(df)
        self.datos[client_id][csv][DATOS].append(df)
        self.transaction.commit(DATA, [client_id, csv, df, self.datos[client_id][csv][LINEAS]])

        if not self.termino_movies[client_id][consulta_id]:
            bsize = BATCH_RATINGS if csv == "ratings" else BATCH_CREDITS
            if self.datos[client_id][csv][LINEAS] >= bsize:
                with self.locks[client_id][csv]:
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
                self.transaction.commit(DISK, [client_id, csv, True])



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
        try:
            os.remove(self.file_paths[client_id][csv])
        except Exception as e:
            logging.error(f"No se pudo borrar el archivo temporal {csv}: {e}")
        self.files_on_disk[client_id][csv] = False
        self.transaction.commit(DISK, [client_id, csv, False])



    def procesar_y_enviar_batch_credit(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is None or len(batch) == 0:
            return
        datos_dict = {entry['id']: entry['title'] for entry in datos}
        joined = []
        for row in batch:
            movie_id = row.get('id')
            cast_raw = row.get('cast', '[]')
            try:
                cast_list = ast.literal_eval(cast_raw)
            except (ValueError, SyntaxError):
                cast_list = []

            if movie_id in datos_dict and isinstance(cast_list, list) and len(cast_list) > 0:
                for cast_member in cast_list:
                    actor_name = cast_member.get('name')
                    if actor_name:
                        joined.append({'id': movie_id, 'name': actor_name})
        if len(joined) > 0:
            batch_id = obtener_batch(mensaje)
            self.transaction.commit(ACCION, [batch_id, joined, ENVIAR])
            enviar_func(canal, destino, joined, mensaje, "RESULT")
            self.transaction.commit(ACCION, [batch_id, "", NO_ENVIAR])


    def procesar_y_enviar_batch_ratings(self, batch, datos, canal, destino, mensaje, enviar_func):
        if batch is None or len(batch) == 0:
            return

        datos_dict = {entry["id"]: entry["title"] for entry in datos}
        joined = []

        for row in batch:
            movie_id = row.get("id")
            if movie_id in datos_dict:
                merged = {
                    "id": movie_id,
                    "title": datos_dict[movie_id],
                    **row
                }
                joined.append(merged)

        if len(joined) > 0:
            batch_id = obtener_batch(mensaje)
            self.transaction.commit(ACCION, [batch_id, joined, ENVIAR])
            enviar_func(canal, destino, joined, mensaje, "RESULT")
            self.transaction.commit(ACCION, [batch_id, "", NO_ENVIAR])


    def borrar_info(self, csv, client_id):
        self.datos[client_id][csv][DATOS] = []
        self.datos[client_id][csv][LINEAS] = 0
        self.transaction.commit(DATA, [client_id, csv, "BORRADO", "BORRADO"])

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
        self.transaction.commit(DISK, [client_id, csv, False])
        self.transaction.commit(RESULT, [client_id, consulta_id, "BORRADO"])


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
        if not ratings:
            return False

        datos_dict = {d["id"]: d["title"] for d in datos}
        resultado = []

        for r in ratings:
            movie_id = r.get("id")
            if movie_id in datos_dict:
                merged = {
                    "id": movie_id,
                    "title": datos_dict[movie_id],
                    **r
                }
                resultado.append(merged)
        return resultado


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
        return exploded



    def procesar_datos(self, consulta_id, tipo_mensaje, mensaje, client_id):
        contenido = obtener_body(mensaje)
        if client_id not in self.datos:
            self.crear_datos(client_id)
        if tipo_mensaje == "MOVIES":
            self.guardar_datos(consulta_id, contenido, client_id)
        else:
            self.almacenar_csv(consulta_id, contenido, client_id)


    def procesar_resultado(self, consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id):
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
            self.transaction.commit(ACCION, [batch_id, resultado, ENVIAR])
            enviar_func(canal, destino, resultado, mensaje, "RESULT")
            self.transaction.commit(ACCION, [batch_id, "", NO_ENVIAR])

            if consulta_id == 3 and self.files_on_disk[client_id]["ratings"]:
                self.enviar_resultados_disco(datos, client_id, canal, destino, mensaje, enviar_func, consulta_id)
            elif consulta_id == 4 and self.files_on_disk[client_id]["credits"]:
                self.enviar_resultados_disco(datos, client_id, canal, destino, mensaje, enviar_func, consulta_id)

        if self.termino_movies[client_id][consulta_id]:
            if self.datos[client_id]["ratings"][TERMINO] or self.datos[client_id]["credits"][TERMINO]:
                self.limpiar_consulta(client_id, consulta_id)

    def enviar_eof(self, consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id):
        if self.termino_movies[client_id][consulta_id] and (
            (consulta_id == 3 and (self.datos[client_id]["ratings"][TERMINO]))
            or (consulta_id == 4 and (self.datos[client_id]["credits"][TERMINO]))
        ):
            self.transaction.commit(ACCION, [batch_id, EOF, ENVIAR])
            enviar_func(canal, destino, EOF, mensaje, EOF)
            self.transaction.commit(ACCION, [batch_id, "", NO_ENVIAR])


    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        batch_id = obtener_batch(mensaje)
        try:
            if tipo_mensaje == "EOF":
                logging.info(f"Se recibio todo Movies, consulta {consulta_id} recibió EOF")
                self.termino_movies[client_id][consulta_id] = True
                self.transaction.commit(TERM, [client_id, consulta_id, True])
                self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
            elif tipo_mensaje in ["MOVIES", "RATINGS", "CREDITS"]:
                self.procesar_datos(consulta_id, tipo_mensaje, mensaje, client_id)
                if tipo_mensaje != "MOVIES":
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
            elif tipo_mensaje == "EOF_RATINGS":
                logging.info("Recibí todos los ratings")
                self.datos[client_id]["ratings"][TERMINO] = True
                self.transaction.commit(DATA_TERM, [client_id, "ratings", True])
                print(f"y esto {self.termino_movies[client_id][consulta_id]}", flush=True)
                if self.datos[client_id]["ratings"][LINEAS] != 0 or self.files_on_disk[client_id]["ratings"]:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
            elif tipo_mensaje == "EOF_CREDITS":
                logging.info("Recibí todos los credits")
                self.datos[client_id]["credits"][TERMINO] = True
                self.transaction.commit(DATA_TERM, [client_id, "credits", True])
                if self.datos[client_id]["credits"][LINEAS] != 0 or self.files_on_disk[client_id]["credits"]:
                    self.procesar_resultado(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
                self.enviar_eof(consulta_id, canal, destino, mensaje, enviar_func, client_id, batch_id)
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

