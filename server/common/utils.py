import ast
from datetime import datetime
import io
import logging
import signal
import sys
import pika # type: ignore
from io import StringIO
import os
import configparser
from pathlib import Path
import csv



def get_batches(worker):
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'batches.ini')
    config.read(config_path)

    v1 = int(config['DEFAULT']['BATCH_CREDITS'])
    v2 = int(config['DEFAULT']['BATCH_RATINGS'])    
    v3 = int(config['DEFAULT']['BATCH_PNL'])    
    if worker == "pnl":
        return v3
    if worker == "joiner":
        return v1, v2

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def graceful_quit(conexion, canal, nodo):
    def shutdown_handler(_, __):
        logging.info("Apagando nodo")
        try:
            es_global = os.path.exists("/tmp/shutdown_global.flag")
            print(f"El shutdown es global {es_global}", flush=True)
            nodo.eliminar(es_global) 
            logging.info("Datos eliminados del nodo.")
        except Exception as e:
            logging.error(f"Error al eliminar datos del nodo: {e}")
        try:
            if canal.is_open:
                canal.close()
            if conexion.is_open:
                conexion.close()
        except Exception as e:
            logging.error(f"Error cerrando conexión/canal: {e}")
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

def borrar_contenido_carpeta(ruta):
    for nombre in os.listdir(ruta):
        ruta_completa = os.path.join(ruta, nombre)
        if os.path.isdir(ruta_completa):
            borrar_contenido_carpeta(ruta_completa)
            os.rmdir(ruta_completa)
        else:
            os.remove(ruta_completa)


def lista_dicts_a_csv(lista):
    if not lista:
        return ""
    if lista == "EOF" or isinstance(lista, str):
        return lista
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=lista[0].keys())
    writer.writeheader()
    writer.writerows(lista)
    return output.getvalue()



# -------------------
# PREPARAR DATA
# -------------------


def create_dataframe(data_str):
    return list(csv.DictReader(StringIO(data_str)))


def concat_data(data):
    resultado = []
    for sublista in data:
        resultado.extend(sublista)
    return resultado


def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)  
        if isinstance(dictionary_list, list):
            return [data['name'] for data in dictionary_list if isinstance(data, dict) and 'name' in data]
        else:
            return []
    except (ValueError, SyntaxError):
        return [] 


def prepare_data_consult_1_3_4(data):
    rows = create_dataframe(data)
    for row in rows:
        # Parsear fecha
        try:
            row["release_date"] = datetime.strptime(row.get("release_date", ""), "%Y-%m-%d")
        except (ValueError, TypeError):
            row["release_date"] = None
        
        # Parsear géneros y países
        row["genres"] = dictionary_to_list(row.get("genres", "[]"))
        row["production_countries"] = dictionary_to_list(row.get("production_countries", "[]"))

        # Guardar como string para búsqueda rápida
        row["genres_str"] = ", ".join(row["genres"]).lower()
        row["production_countries_str"] = ", ".join(row["production_countries"]).lower()
    return rows


def prepare_data_consult_2(data):
    data = create_dataframe(data)
    resultado = []
    for i, row in enumerate(data):
        pc_str = row.get('production_countries', '[]')
        row['production_countries'] = dictionary_to_list(pc_str)

        try:
            row['budget'] = float(row.get('budget', 0))
        except (ValueError, TypeError):
            row['budget'] = 0
        
        resultado.append(row)
    return resultado


def prepare_data_consult_4(data):
    if any(isinstance(el, list) for el in data):
        data = [item for sublist in data for item in (sublist if isinstance(sublist, list) else [sublist])]

    for entry in data:
        cast_raw = entry.get('cast', '[]')
        if isinstance(cast_raw, str):
            try:
                cast_list = ast.literal_eval(cast_raw)
                entry['cast'] = cast_list if isinstance(cast_list, list) else []
            except Exception:
                entry['cast'] = []
        elif isinstance(cast_raw, list):
            entry['cast'] = cast_raw
        else:
            entry['cast'] = []

    return data




def prepare_data_consult_5(data):
    datos = create_dataframe(data)
    resultado_filtrado = []
    for row in datos:
        try:
            budget = int(row['budget'])
            revenue = int(row['revenue'])
        except (ValueError, KeyError):
            continue  # saltar si no es convertible a int o faltan campos

        if budget != 0 and revenue != 0:
            resultado_filtrado.append(row)

    return resultado_filtrado


def prepare_data_aggregator_consult_3(min_result, max_result):
    headers = ["id", "title", "rating"]
    data = [
        {**{h: max_result[h] for h in headers}, "tipo": "max"},
        {**{h: min_result[h] for h in headers}, "tipo": "min"}
    ]
    return data






# -------------------
# EOF
# -------------------
EOF = "EOF"

def cargar_datos_broker():
    diccionario = cargar_broker()
    a_enviar = cargar_eof_a_enviar()
    diccionario_invertido = {}
    
    for clave, valores in diccionario.items():
        for valor in valores:
            if valor not in diccionario_invertido:
                diccionario_invertido[valor] = []
            diccionario_invertido[valor].append(clave)
    
    return diccionario_invertido | {5: a_enviar[5]}


def cargar_broker():
    raw = os.getenv("JOINERS", "")
    eofs = {}
    if raw:
        for par in raw.split(";"):
            if ":" in par:
                k, v = par.split(":")
                value = ast.literal_eval(v)
                if not isinstance(value, list):
                    value = [value] 
                eofs[int(k)] = value
    return eofs


def cargar_eofs_joiners():
    raw = os.getenv("EOF_ENVIAR", "")
    eofs = {}
    if raw:
        for par in raw.split(","):
            if ":" in par:
                k, v = par.split(":")
                eofs[int(k)] = int(v)
    return eofs

def cargar_eof_a_enviar():
    raw = os.getenv("EOF_ENVIAR")
    eofs = {}
    if raw:
        for par in raw.split(","):
            if ":" in par:
                k, v = par.split(":")
                eofs[int(k)] = int(v)
    return eofs



def cargar_eofs():
    raw = os.getenv("EOF_ESPERADOS", "")
    eofs = {}
    if raw:
        for par in raw.split(","):
            if ":" in par:
                k, v = par.split(":")
                eofs[int(k)] = int(v)
    return eofs

def cargar_nodo_siguiente():
    return os.getenv("NODO_SIGUIENTE", "")

def cargar_nodo_anterior():
    return os.getenv("NODO_ANTERIOR", "")

def cargar_puerto():
    return os.getenv("PUERTO", "")

def cargar_puerto_siguiente():
    return os.getenv("PUERTO_SIGUIENTE", "")

def obtiene_nombre_contenedor(tipo):
    worker_id = int(os.environ.get("WORKER_ID", 0))
    if tipo != "broker":
        worker_id = int(os.environ.get("WORKER_ID", 0))
        nombre_nodo = f"{tipo}{worker_id}"
    else:
        nombre_nodo = "broker"
    return nombre_nodo

def obtener_batch(mensaje):
    return mensaje['headers'].get("batchID")


# -------------------
# NORMALIZATION
# -------------------

def write_dicts_to_csv(file_path, data, append=False):
    if not data:
        return
    fieldnames = data[0].keys()
    mode = 'a' if append else 'w'
    with open(file_path, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not append:
            writer.writeheader()
        writer.writerows(data)


def parse_datos(s, convertir_numeros=False):
    s = s.strip()
    if not s.startswith("[") or not s.endswith("]"):
        raise ValueError("No es una lista válida")

    items = []
    actual = ""
    nivel = 0
    dentro = False

    for c in s[1:-1]:
        if c == '{':
            nivel += 1
            dentro = True
        if dentro:
            actual += c
        if c == '}':
            nivel -= 1
            if nivel == 0:
                dentro = False
                items.append(actual.strip())
                actual = ""

    lista_dicts = []
    for item in items:
        d = {}
        item = item.strip("{}")
        for par in item.split(", "):
            if ": " not in par:
                continue
            k, v = par.split(": ", 1)
            k = k.strip("'\" ")
            v = v.strip("'\" ")
            if convertir_numeros:
                # Si v parece tener caracteres extraños no intentes convertir
                if v == "None":
                    v = None
                elif all(c.isdigit() for c in v):
                    v = int(v)
                else:
                    try:
                        v_float = float(v)
                        v = v_float
                    except ValueError:
                        # No convertible, lo dejamos string tal cual
                        pass

            d[k] = v
        lista_dicts.append(d)

    return lista_dicts


def limpiar_cast_string(cast_str):
    # Reemplazar comillas simples literales (\' => ')  
    # y también convertir cualquier escape excesivo de barras invertidas
    # Para que quede una string parseable.

    # 1. Reemplazo \' por ' simple:
    cast_str = cast_str.replace("\\'", "'")
    # 2. Reemplazo \" por " (por si hay)
    cast_str = cast_str.replace('\\"', '"')
    # 3. Reemplazo dobles barras invertidas \\ por una sola \
    cast_str = cast_str.replace('\\\\', '\\')
    return cast_str

def parse_credits(s):
    s = s.strip()
    if not s.startswith("[") or not s.endswith("]"):
        raise ValueError("No es una lista válida de películas")

    items = []
    actual = ""
    nivel = 0
    dentro = False

    for c in s[1:-1]:
        if c == '{':
            nivel += 1
            dentro = True
        if dentro:
            actual += c
        if c == '}':
            nivel -= 1
            if nivel == 0:
                dentro = False
                items.append(actual.strip())
                actual = ""

    resultado = []
    for item in items:
        d = {}
        item = item.strip("{}")
        for par in item.split(", ", 1):  
            if ": " not in par:
                continue
            k, v = par.split(": ", 1)
            k = k.strip("'\" ")
            v = v.strip("'\" ")
            d[k] = v

        if "cast" in d:
            try:
                limpio = limpiar_cast_string(d['cast'])
                d["cast"] = f"{parse_datos(limpio, True)}"
            except Exception as e:
                d["cast"] = f"{[]}" 
        resultado.append(d)

    return resultado
