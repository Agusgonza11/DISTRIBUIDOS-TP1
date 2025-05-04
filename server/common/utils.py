import ast
import logging
import signal
import sys
import pika # type: ignore
from io import StringIO
import pandas as pd # type: ignore
import os


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
            nodo.eliminar() 
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


# -------------------
# PANDAS
# -------------------
def create_dataframe(csv):
    return pd.read_csv(StringIO(csv))

def create_body(body):
    if isinstance(body, pd.DataFrame):
        return body.to_csv(index=False)
    else:
        return body

def puede_enviar(body):
    if isinstance(body, pd.DataFrame):
        return not body.empty
    return bool(body)

def concat_data(data):
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)  
        if isinstance(dictionary_list, list):
            return [data['name'] for data in dictionary_list if isinstance(data, dict) and 'name' in data]
        else:
            return []
    except (ValueError, SyntaxError):
        return [] 



# -------------------
# PREPARAR DATA
# -------------------

def prepare_data_consult_1_3_4(data):
    data = create_dataframe(data)
    data['release_date'] = pd.to_datetime(data['release_date'], format='%Y-%m-%d', errors='coerce')
    data['genres'] = data['genres'].apply(dictionary_to_list)
    data['production_countries'] = data['production_countries'].apply(dictionary_to_list)
    data['genres'] = data['genres'].astype(str)
    data['production_countries'] = data['production_countries'].astype(str)
    return data

def prepare_data_consult_2(data):
    data = create_dataframe(data)
    data['production_countries'] = data['production_countries'].apply(dictionary_to_list)
    data['production_countries'] = data['production_countries'].astype(str)
    data['budget'] = pd.to_numeric(data['budget'], errors='coerce')
    return data

def prepare_data_consult_4(data):
    credits = concat_data(data) 
    credits['cast'] = credits['cast'].apply(dictionary_to_list)
    return credits

def prepare_data_consult_5(data):
    datos = create_dataframe(data)
    datos['budget'] = pd.to_numeric(datos['budget'], errors='coerce')
    datos['revenue'] = pd.to_numeric(datos['revenue'], errors='coerce')
    return datos


def prepare_data_aggregator_consult_3(min, max):
    headers = ["id", "title", "rating"]
    data = [
        {**{h: max[h] for h in headers}, "tipo": "max"},
        {**{h: min[h] for h in headers}, "tipo": "min"}
    ]
    return pd.DataFrame(data)





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

########################
# NORMALIZATION
########################

def normalize_ratings_df(df):
    df['id'] = df['id'].astype(str)
    return df

def normalize_credits_df(df):
    df['id'] = df['id'].astype(str)
    if 'cast' in df.columns:
        df['cast'] = df['cast'].apply(dictionary_to_list)
    return df

def normalize_movies_df(df):
    if 'id' in df.columns:
        df['id'] = df['id'].astype(str)
    return df
