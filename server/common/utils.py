import ast
import logging
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
    return pd.concat(data, ignore_index=True)

def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)  
        return [data['name'] for data in dictionary_list]  
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

def cargar_eofs():
    raw = os.getenv("EOF_ESPERADOS", "")
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
