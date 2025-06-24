import ast
import csv
import logging
import os
import tempfile
import threading
from common.utils import EOF, concat_data, create_dataframe, get_batch_limits, obtener_message_id, obtener_nombre_contenedor, parse_credits, parse_datos, prepare_data_consult_4, write_dicts_to_csv
from common.communication import obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from common.exceptions import ConsultaInexistente, ErrorCargaDelEstado
from common.transaction import ACCION, Transaction


JOINER = "joiner"

DATOS = 0
LINEAS = 1
TERMINO = 2
TMP_DIR = f"/tmp/{obtener_nombre_contenedor(JOINER)}_tmp"

BATCH_CREDITS, BATCH_RATINGS = get_batch_limits(JOINER)

DATA = "datos"
DATA_TERM = "termino_datos"
TERM = "termino_movies"
RESULT = "resultados_parciales"
DISK = "archivos_en_disco"
PATH = "archivos_path"

NO_ENVIAR = "no_enviar"
ENVIAR = "enviar"

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:

    def __init__(self):
        self.informacion_movies = {}
        self.termino_movies = {}
        self.informacion_csvs = {}
        self.locks = {}
        self.archivos_en_disco = {}
        self.archivos_path = {}
        self.transaction = Transaction(TMP_DIR)
        if self.transaction.cargar_estado(self):
            for client_id in self.informacion_movies.keys():
                self.transaction.commit(PATH, [client_id, self.archivos_path[client_id]["ratings"], self.archivos_path[client_id]["credits"]])


    def reconstruir_estado(self, clave, contenido):
        partes = contenido.split("|")
        if len(partes) < 2:
            raise ErrorCargaDelEstado(f"Contenido malformado: {contenido!r}")
        client_id, *contenido = partes
        if client_id not in self.informacion_movies:
            self.crear_datos(client_id, True)
            
        match clave:
            case "datos":
                csv_almacenado, datos, lineas = contenido
                if datos == "BORRADO":
                    self.informacion_csvs[client_id][csv_almacenado][DATOS] = []
                    self.informacion_csvs[client_id][csv_almacenado][LINEAS] = 0
                else:
                    try:
                        datos_dict = ast.literal_eval(datos)
                    except Exception as e:
                        logging.error(f"No se pudo parsear 'datos' con ast.literal_eval: {datos}")
                        raise

                    content = datos_dict.get('datos', None)
                    batch_id = datos_dict.get('batch_id', None)
                    if content is None:
                        return

                    
                    if csv_almacenado == "ratings":
                        logging.info("Joiner recibe datos de ratings")
                        valor = parse_datos(content)
                    elif csv_almacenado == "credits":
                        logging.info("Joiner recibe datos de credits")
                        valor = parse_credits(content)

                    self.informacion_csvs[client_id][csv_almacenado][DATOS].append({"batch_id": batch_id, "datos": content})
                    self.informacion_csvs[client_id][csv_almacenado][LINEAS] = int(lineas)

            case "termino_datos":
                csv_almacenado, booleano = contenido
                valor = booleano == "True"
                self.informacion_csvs[client_id][csv_almacenado][TERMINO] = valor
            case "termino_movies":
                request_id, booleano = contenido
                request_id = int(request_id)
                valor = booleano == "True"
                self.termino_movies[client_id][request_id] = valor
            case "resultados_parciales":
                request_id, valor = contenido
                request_id = int(request_id)
                if valor == "BORRADO":
                    self.informacion_movies[client_id][request_id] = []
                else:
                    self.informacion_movies[client_id][request_id].append(parse_datos(valor))
            case "archivos_en_disco":
                csv_almacenado, booleano = contenido
                valor = booleano == "True"
                self.archivos_en_disco[client_id][csv_almacenado] = valor
            case "archivos_path":
                ratings, credits = contenido
                if not client_id in self.archivos_path:
                    self.archivos_path[client_id] = {"ratings": None, "credits": None}
                self.archivos_path[client_id]["ratings"] = ratings
                self.archivos_path[client_id]["credits"] = credits
            case _:
                raise ErrorCargaDelEstado(f"Error en la carga del estado")


    def eliminar(self, es_global):
        if es_global:
            try:
                self.transaction.borrar_carpeta()
                for client_id, paths in self.archivos_path.items():
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
        
        self.informacion_movies.clear()
        self.termino_movies.clear()
        self.informacion_csvs.clear()
        self.locks.clear()
        self.archivos_en_disco.clear()
        self.archivos_path.clear()



    def crear_datos(self, client_id, reiniciado=None):
        self.informacion_csvs[client_id] = {"ratings": [[], 0, False], "credits": [[], 0, False]}
        self.termino_movies[client_id] = {
            3: False,
            4: False,
        }
        self.informacion_movies[client_id] = {
            3: [],
            4: [],
        }

        self.locks[client_id] = {
            "ratings": threading.Lock(),
            "credits": threading.Lock(),
        }
        
        self.archivos_en_disco[client_id] = {
            "ratings": False,
            "credits": False,
        }

        os.makedirs(TMP_DIR, exist_ok=True)

        if reiniciado == True: return

        self.archivos_path[client_id] = {
            "ratings": tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR).name,
            "credits": tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR).name,
        }

        self.transaction.commit(PATH, [client_id, self.archivos_path[client_id]["ratings"], self.archivos_path[client_id]["credits"]])


    def consulta_habilitada(self, request_id, client_id):
        if not self.termino_movies[client_id][request_id]:
            return False
        if request_id == 3:
            return self.informacion_csvs[client_id]["ratings"][TERMINO] or (self.informacion_csvs[client_id]["ratings"][LINEAS] >= BATCH_RATINGS) or self.archivos_en_disco[client_id]["ratings"]
        if request_id == 4:
            return self.informacion_csvs[client_id]["credits"][TERMINO] or (self.informacion_csvs[client_id]["credits"][LINEAS] >= BATCH_CREDITS) or self.archivos_en_disco[client_id]["credits"]

        return False

    def handlear_movies_batch(self, request_id, datos, client_id):
        df = create_dataframe(datos)
        self.informacion_movies[client_id][request_id].append(df)
        self.transaction.commit(RESULT, [client_id, request_id, df])


    def almacenar_csv(self, request_id, datos, client_id, mensaje):
        csv = "ratings" if request_id == 3 else "credits"
        batch_id = mensaje['headers'].get('BatchID')

        df = create_dataframe(datos)
        self.informacion_csvs[client_id][csv][LINEAS] += len(df)

        data_dict = {"batch_id": batch_id, "datos": df}
        self.informacion_csvs[client_id][csv][DATOS].append(data_dict)

        self.transaction.commit(DATA, [client_id, csv, data_dict, self.informacion_csvs[client_id][csv][LINEAS]])

        if not self.termino_movies[client_id][request_id]:
            bsize = BATCH_RATINGS if csv == "ratings" else BATCH_CREDITS
            if self.informacion_csvs[client_id][csv][LINEAS] >= bsize:
                with self.locks[client_id][csv]:
                    df_total = []

                    for chunk in self.informacion_csvs[client_id][csv][DATOS]:
                        batch_id = chunk["batch_id"]
                        for row in chunk["datos"]:
                            row["_batch_id"] = batch_id
                            df_total.append(row)

                    write_dicts_to_csv(
                        self.archivos_path[client_id][csv],
                        df_total,
                        append=self.archivos_en_disco[client_id][csv]
                    )
                    self.archivos_en_disco[client_id][csv] = True


                self.borrar_info(csv, client_id)
                self.transaction.commit(DISK, [client_id, csv, True])



    def leer_batches_de_disco(self, client_id, csv_name):
        batch_size = BATCH_RATINGS if csv_name == "ratings" else BATCH_CREDITS
        file_path = self.archivos_path[client_id][csv_name]

        with self.locks[client_id][csv_name]:
            if not os.path.exists(file_path):
                logging.info(f"Filepath doesn't exist {file_path}")
                return

            with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                batch = []
                ultimo_batch_id = None
                for row in reader:
                    batch_id = row.pop('_batch_id', None)

                    if ultimo_batch_id is None:
                        ultimo_batch_id = batch_id
                    if batch_id != ultimo_batch_id or len(batch) >= batch_size:
                        if batch:
                            yield batch, ultimo_batch_id
                        batch = []
                        ultimo_batch_id = batch_id
                    batch.append(row)
                if batch:
                    yield batch, ultimo_batch_id


    def enviar_resultados_disco(self, datos, client_id, canal, destino, mensaje, enviar_func, request_id):
        csv = "ratings" if request_id == 3 else "credits"
        for batch, batch_id in self.leer_batches_de_disco(client_id, csv):
            batch_mensaje = mensaje.copy()
            batch_mensaje['headers'] = batch_mensaje.get('headers', {}).copy()
            batch_mensaje['headers']['MessageID'] = str(batch_id)

            if request_id == 3:
                self.procesar_y_enviar_batch_ratings(batch, datos, canal, destino, mensaje, enviar_func, batch_id)
            else:
                self.procesar_y_enviar_batch_credit(batch, datos, canal, destino, mensaje, enviar_func, batch_id)
            batch = None
        try:
            os.remove(self.archivos_path[client_id][csv])
        except Exception as e:
            logging.error(f"No se pudo borrar el archivo temporal {csv}: {e}")

        self.archivos_en_disco[client_id][csv] = False
        self.transaction.commit(DISK, [client_id, csv, False])

    def procesar_y_enviar_batch_credit(self, batch, informacion_movies, canal, destino, mensaje, enviar_func, message_id):
        if batch is None or len(batch) == 0:
            return

        movies_dict = {entry['id']: entry['title'] for entry in informacion_movies}

        resultado_final = []

        for row in batch:
            movie_id = row.get('id')
            cast_raw = row.get('cast', '[]')

            try:
                cast_list = ast.literal_eval(cast_raw)
            except (ValueError, SyntaxError):
                cast_list = []

            if movie_id in movies_dict and isinstance(cast_list, list) and len(cast_list) > 0:
                for miembro in cast_list:
                    nombre_actor = miembro.get('name')
                    if nombre_actor:
                        resultado_final.append({'id': movie_id, 'name': nombre_actor})

        if len(resultado_final) > 0:
            self.transaction.commit(ACCION, [message_id, resultado_final, ENVIAR])
            enviar_func(canal, destino, resultado_final, mensaje, "RESULT")
            self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])


    def procesar_y_enviar_batch_ratings(self, batch, informacion_movies, canal, destino, mensaje, enviar_func, message_id):
        if batch is None or len(batch) == 0:
            return

        movies_dict = {entry["id"]: entry["title"] for entry in informacion_movies}
        resultado_final = []

        for row in batch:
            movie_id = row.get("id")
            if movie_id in movies_dict:
                merged = {
                    "id": movie_id,
                    "title": movies_dict[movie_id],
                    **row
                }
                resultado_final.append(merged)

        if len(resultado_final) > 0:
            self.transaction.commit(ACCION, [message_id, resultado_final, ENVIAR])
            enviar_func(canal, destino, resultado_final, mensaje, "RESULT")
            self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])


    def borrar_info(self, csv, client_id):
        self.informacion_csvs[client_id][csv][DATOS] = []
        self.informacion_csvs[client_id][csv][LINEAS] = 0
        self.transaction.commit(DATA, [client_id, csv, "BORRADO", "BORRADO"])

    def limpiar_consulta(self, client_id, request_id):
        csv = "ratings" if request_id == 3 else "credits"
        path = self.archivos_path[client_id][csv]
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as ex:
                logging.error(f"No se pudo borrar ratings temp: {ex}")
        self.archivos_en_disco[client_id][csv] = False
        self.archivos_path[client_id][csv] = ""
        self.borrar_info(csv, client_id)
        self.informacion_movies[client_id][request_id] = []
        self.transaction.commit(DISK, [client_id, csv, False])
        self.transaction.commit(RESULT, [client_id, request_id, "BORRADO"])


    def ejecutar_consulta(self, informacion_movies, request_id, client_id):
        match request_id:
            case 3:
                return self.ejecutar_consulta_3(informacion_movies, client_id)
            case 4:
                return self.ejecutar_consulta_4(informacion_movies, client_id)
            case _:
                logging.warning(f"Consulta desconocida: {request_id}")
                raise ConsultaInexistente(f"Consulta {request_id} no encontrada")

    def ejecutar_consulta_3(self, informacion_movies, client_id):
        logging.info(f"[CONSULTA 3 - {client_id}] Ejecutando")    

        ratings = concat_data(self.informacion_csvs[client_id]["ratings"][DATOS])
        self.borrar_info("ratings", client_id)
        if not ratings:
            return False

        movies_dict = {d["id"]: d["title"] for d in informacion_movies}
        resultado = []

        for r in ratings:
            movie_id = r.get("id")
            if movie_id in movies_dict:
                merged = {
                    "id": movie_id,
                    "title": movies_dict[movie_id],
                    **r
                }
                resultado.append(merged)

        return resultado


    def ejecutar_consulta_4(self, informacion_movies, client_id):
        logging.info(f"[CONSULTA 4 - {client_id}] Ejecutando")    

        credits = prepare_data_consult_4(concat_data(self.informacion_csvs[client_id]["credits"][DATOS]))
        self.borrar_info("credits", client_id)

        if not credits:
            return False
        
        lista_movies = [ {"id": d["id"], "title": d["title"]} for d in informacion_movies ]

        merged = []

        credits_by_id = {}

        for c in credits:
            credits_by_id.setdefault(c["id"], []).append(c)

        for movie in lista_movies:
            movie_id = movie["id"]
            if movie_id in credits_by_id:
                for credit in credits_by_id[movie_id]:
                    merged.append({**movie, **credit})
                    
        resultado_final = []

        for m in merged:
            cast_list = m.get('cast', [])
            for miembro in cast_list:
                if isinstance(miembro, dict):
                    nombre_actor = miembro.get("name")
                else:
                    nombre_actor = miembro
                if nombre_actor:
                    resultado_final.append({
                        "id": m["id"],
                        "name": nombre_actor
                    })

        return resultado_final



    def handlear_mensaje(self, request_id, tipo_mensaje, mensaje, client_id):
        contenido = obtener_body(mensaje)
        if client_id not in self.informacion_csvs:
            self.crear_datos(client_id)
        if tipo_mensaje == "MOVIES":
            self.handlear_movies_batch(request_id, contenido, client_id)
        else:
            self.almacenar_csv(request_id, contenido, client_id, mensaje)


    def procesar_resultado(self, request_id, canal, destino, mensaje, enviar_func, client_id):
        if self.consulta_habilitada(request_id, client_id):
            if client_id not in self.informacion_movies:
                return False
            
            datos_cliente = self.informacion_movies[client_id]
            if not datos_cliente or request_id not in datos_cliente:
                return False
            
            datos = concat_data(datos_cliente[request_id])

            if request_id == 3:
                batch_info = self.informacion_csvs[client_id]["ratings"][DATOS]
            else:
                batch_info = self.informacion_csvs[client_id]["credits"][DATOS]

            batch_ids = [str(b.get('batch_id')) for b in batch_info if 'batch_id' in b]
            batch_ids = sorted(set(batch_ids))  
            message_id = "-".join(batch_ids)

            mensaje['headers']['MessageID'] = message_id

            resultado = self.ejecutar_consulta(datos, request_id, client_id)

            self.transaction.commit(ACCION, [message_id, resultado, ENVIAR])
            enviar_func(canal, destino, resultado, mensaje, "RESULT")
            self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])

            if request_id == 3 and self.archivos_en_disco[client_id]["ratings"]:
                self.enviar_resultados_disco(datos, client_id, canal, destino, mensaje, enviar_func, request_id)
            elif request_id == 4 and self.archivos_en_disco[client_id]["credits"]:
                self.enviar_resultados_disco(datos, client_id, canal, destino, mensaje, enviar_func, request_id)

        if self.termino_movies[client_id][request_id]:
            if self.informacion_csvs[client_id]["ratings"][TERMINO] or self.informacion_csvs[client_id]["credits"][TERMINO]:
                self.limpiar_consulta(client_id, request_id)

    def enviar_eof(self, request_id, canal, destino, mensaje, enviar_func, client_id):
        if self.termino_movies[client_id][request_id] and (
            (request_id == 3 and (self.informacion_csvs[client_id]["ratings"][TERMINO]))
            or (request_id == 4 and (self.informacion_csvs[client_id]["credits"][TERMINO]))
        ):
            message_id = f"{client_id}-{request_id}"
            mensaje['headers']['MessageID'] = message_id

            self.transaction.commit(ACCION, [message_id, EOF, ENVIAR])
            enviar_func(canal, destino, EOF, mensaje, EOF)
            self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])


    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        request_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        message_id = obtener_message_id(mensaje)
        if tipo_mensaje in ["RATINGS", "CREDITS"]:
            logging.info(f"Mensaje {message_id} recibido")

        if self.transaction.mensaje_duplicado(client_id, message_id, enviar_func, mensaje, canal, destino):
            logging.info(f"Mensaje {message_id} ya procesado, ignorando")
            return
        
        try:
            if tipo_mensaje == "EOF":
                logging.info(f"EOF Movies: {request_id} para el cliente {client_id}")

                self.termino_movies[client_id][request_id] = True
                self.transaction.commit(TERM, [client_id, request_id, True])
                self.procesar_resultado(request_id, canal, destino, mensaje, enviar_func, client_id)
                self.enviar_eof(request_id, canal, destino, mensaje, enviar_func, client_id)
         
            elif tipo_mensaje in ["MOVIES", "RATINGS", "CREDITS"]:
                self.handlear_mensaje(request_id, tipo_mensaje, mensaje, client_id)
                if tipo_mensaje != "MOVIES":
                    self.procesar_resultado(request_id, canal, destino, mensaje, enviar_func, client_id)

            elif tipo_mensaje == "EOF_RATINGS":
                logging.info(f"EOF Ratings: {request_id} para el cliente {client_id}")
               
                self.informacion_csvs[client_id]["ratings"][TERMINO] = True
                self.transaction.commit(DATA_TERM, [client_id, "ratings", True])
                
                if self.informacion_csvs[client_id]["ratings"][LINEAS] > 0:
                    self.procesar_resultado(request_id, canal, destino, mensaje, enviar_func, client_id)

                self.enviar_eof(request_id, canal, destino, mensaje, enviar_func, client_id)

            elif tipo_mensaje == "EOF_CREDITS":
                logging.info(f"EOF Credits: {request_id} para el cliente {client_id}")

                self.informacion_csvs[client_id]["credits"][TERMINO] = True
                self.transaction.commit(DATA_TERM, [client_id, "credits", True])

                if self.informacion_csvs[client_id]["credits"][LINEAS] > 0:
                    self.procesar_resultado(request_id, canal, destino, mensaje, enviar_func, client_id)

                self.enviar_eof(request_id, canal, destino, mensaje, enviar_func, client_id)

            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {request_id}: {e}")


# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    run(JOINER, JoinerNode)

