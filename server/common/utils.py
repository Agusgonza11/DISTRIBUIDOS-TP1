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


def prepare_data_filter(data):
    data = create_dataframe(data)
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    return data

def prepare_data_aggregator_consult_3(min, max):
    headers = ["id", "title", "rating"]
    lines = [",".join(headers)]

    for fila in [max, min]:
        linea = ",".join(str(fila[h]) for h in headers)
        lines.append(linea)

    return "\n".join(lines)

def cargar_eofs():
    raw = os.getenv("EOF_ESPERADOS", "")
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