import logging
import aio_pika
import asyncio
from io import StringIO
import pandas as pd


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


def prepare_data_filter_consult_1(data):
    data = create_dataframe(data)
    data['release_date'] = pd.to_datetime(data['release_date'], errors='coerce')
    return data

async def esperar_conexion():
    for i in range(10):
        try:
            conexion = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
            return conexion
        except Exception as e:
            logging.warning(f"Intento {i+1}: No se pudo conectar a RabbitMQ: {e}")
            await asyncio.sleep(2)
    raise Exception("No se pudo conectar a RabbitMQ después de varios intentos")