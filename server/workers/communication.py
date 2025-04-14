import aio_pika
import logging
from common.utils import esperar_conexion


conexion = None
canal = None

# ---------------------
# GENERALES
# ---------------------
async def inicializar_comunicacion():
    global conexion, canal
    conexion = await esperar_conexion()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=1)


async def enviar_mensaje(routing_key, body):
    await canal.default_exchange.publish(
        aio_pika.Message(body=body.encode()),
        routing_key=routing_key
    )


# ---------------------
# FILTRO
# ---------------------
async def escuchar_colas_para_filtro(procesar_mensajes):
    for consulta_id in range(1, 6):
        nombre_entrada = f"filter_consult_{consulta_id}"
        nombre_salida = f"gateway_output_{consulta_id}" if consulta_id == 1 else f"aggregator_consult_{consulta_id}"

        await canal.declare_queue(nombre_entrada, durable=True)
        await canal.declare_queue(nombre_salida, durable=True)

        async def wrapper(mensaje, consulta_id=consulta_id):
            async with mensaje.process():
                contenido = mensaje.body.decode('utf-8') 
                await procesar_mensajes(consulta_id, contenido, enviar_mensaje)

        queue = await canal.get_queue(nombre_entrada)
        await queue.consume(wrapper)
        logging.info(f"Escuchando en {nombre_entrada}")
