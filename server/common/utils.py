import logging
import aio_pika
import asyncio


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

async def esperar_conexion():
    for i in range(10):
        try:
            conexion = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
            return conexion
        except Exception as e:
            logging.warning(f"Intento {i+1}: No se pudo conectar a RabbitMQ: {e}")
            await asyncio.sleep(2)
    raise Exception("No se pudo conectar a RabbitMQ después de varios intentos")