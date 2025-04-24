import logging
import aio_pika # type: ignore
import asyncio
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


def prepare_data_filter(data):
    data = create_dataframe(data)
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    return data

def prepare_data_aggregator_consult_3(min, max):
    headers = ["id", "title", "rating"]
    data = [
        {**{h: max[h] for h in headers}, "tipo": "max"},
        {**{h: min[h] for h in headers}, "tipo": "min"}
    ]
    return pd.DataFrame(data)


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


async def esperar_conexion():
    for i in range(10):
        try:
            conexion = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
            return conexion
        except Exception as e:
            logging.warning(f"Intento {i+1}: No se pudo conectar a RabbitMQ: {e}")
            await asyncio.sleep(2)
    raise Exception("No se pudo conectar a RabbitMQ después de varios intentos")