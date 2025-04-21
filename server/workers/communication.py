import aio_pika # type: ignore
import logging
from common.utils import create_body, esperar_conexion, puede_enviar


# ----------------------
# Constantes globales
# ----------------------
conexion = None
canal = None

# ----------------------
# Diccionario global
# ----------------------
COLAS = {
    "filter_consult_1": "gateway_output",
    "filter_consult_2": "aggregator_consult_2",
    "filter_consult_3": "joiner_consult_3",
    "filter_consult_4": "joiner_consult_4",
    "filter_consult_5": "pnl_consult_5",
    "aggregator_consult_2": "gateway_output",
    "aggregator_consult_3": "gateway_output",
    "aggregator_consult_4": "gateway_output",
    "aggregator_consult_5": "gateway_output",
    "pnl_consult_5": "aggregator_consult_5",
    "joiner_consult_3": "aggregator_consult_3",
    "joiner_consult_4": "aggregator_consult_4",
}



# ---------------------
# GENERALES
# ---------------------
async def inicializar_comunicacion():
    global conexion, canal
    conexion = await esperar_conexion()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=1)


async def enviar_mensaje(routing_key, body, headers=None):
    if puede_enviar(body):
        logging.info(f"Lo que voy a enviar es {body}")
        await canal.default_exchange.publish(
            aio_pika.Message(body=create_body(body).encode(), headers=headers),
            routing_key=routing_key,
        )
    else:
        logging.info("No se enviará el mensaje: body vacío")




# ---------------------
# GENERAL
# ---------------------
async def escuchar_colas(entrada, nodo, consultas):
    for consulta_id in consultas:
        nombre_entrada = f"{entrada}_consult_{consulta_id}"
        nombre_salida = COLAS[nombre_entrada]

        await canal.declare_queue(nombre_entrada, durable=True)
        await canal.declare_queue(nombre_salida, durable=True)

        async def wrapper(mensaje, consulta_id=consulta_id, nombre_salida=nombre_salida):
            async with mensaje.process():
                await nodo.procesar_mensajes(nombre_salida, consulta_id, mensaje, enviar_mensaje)

        queue = await canal.get_queue(nombre_entrada)
        await queue.consume(wrapper)
        logging.info(f"Escuchando en {nombre_entrada}")



