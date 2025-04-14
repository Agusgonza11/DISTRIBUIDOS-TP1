import aio_pika # type: ignore
import logging
from common.utils import esperar_conexion

# ----------------------
# Constantes globales
# ----------------------
FILTER = 1
AGGREGATOR = 2
PNL = 5
JOINER = 3
LIMITE_NODOS_1 = 6
LIMITE_NODOS_2 = 5
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
    "aggregator_consult_1": "gateway_output",
    "aggregator_consult_2": "gateway_output",
    "aggregator_consult_3": "gateway_output",
    "aggregator_consult_4": "gateway_output",
    "aggregator_consult_5": "gateway_output",
    "pnl_consult_5": "aggregator_consult_5",
    "joiner_consult_3": "aggregator_consult_3",
    "joiner_consult_4": "aggregator_consult_4",
}


ENTRADAS = {
    "filter": FILTER,
    "aggregator": AGGREGATOR,
    "pnl": PNL,
    "joiner": JOINER,
}

SALIDAS = {
    "filter": LIMITE_NODOS_1,
    "aggregator": LIMITE_NODOS_1,
    "pnl": LIMITE_NODOS_1,
    "joiner": LIMITE_NODOS_2,
}

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
# GENERAL
# ---------------------
async def escuchar_colas(entrada, nodo):
    for consulta_id in range(ENTRADAS[entrada], SALIDAS[entrada]):
        nombre_entrada = f"{entrada}_consult_{consulta_id}"
        nombre_salida = COLAS[nombre_entrada]

        await canal.declare_queue(nombre_entrada, durable=True)
        await canal.declare_queue(nombre_salida, durable=True)

        async def wrapper(mensaje, consulta_id=consulta_id, nombre_salida=nombre_salida):
            async with mensaje.process():
                contenido = mensaje.body.decode('utf-8') 
                await procesar_mensajes(nodo, nombre_salida, consulta_id, contenido, enviar_mensaje)

        queue = await canal.get_queue(nombre_entrada)
        await queue.consume(wrapper)
        logging.info(f"Escuchando en {nombre_entrada}")


async def procesar_mensajes(nodo, destino, consulta_id, contenido, enviar_func):
    if contenido.strip() == "EOF":
        logging.info(f"Consulta {consulta_id} recibió EOF")
        await enviar_func(destino, "EOF")
        return
    resultado = nodo.ejecutar_consulta(consulta_id, contenido)
    await enviar_func(destino, resultado)





"""
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


# ---------------------
# AGGREGATOR
# ---------------------
async def escuchar_colas_para_aggregator(procesar_mensajes):
    for consulta_id in range(2, 6):
        nombre_entrada = f"aggregator_consult_{consulta_id}"
        nombre_salida = f"gateway_output_{consulta_id}"

        await canal.declare_queue(nombre_entrada, durable=True)
        await canal.declare_queue(nombre_salida, durable=True)

        async def wrapper(mensaje, consulta_id=consulta_id):
            async with mensaje.process():
                contenido = mensaje.body.decode('utf-8') 
                await procesar_mensajes(consulta_id, contenido, enviar_mensaje)

        queue = await canal.get_queue(nombre_entrada)
        await queue.consume(wrapper)
        logging.info(f"Escuchando en {nombre_entrada}")


# ---------------------
# PNL
# ---------------------
async def escuchar_colas_para_pnl(procesar_mensajes):
    for consulta_id in range(5, 6):
        nombre_entrada = f"pnl_consult_{consulta_id}"
        nombre_salida = f"filter_consult_{consulta_id}"

        await canal.declare_queue(nombre_entrada, durable=True)
        await canal.declare_queue(nombre_salida, durable=True)

        async def wrapper(mensaje, consulta_id=consulta_id):
            async with mensaje.process():
                contenido = mensaje.body.decode('utf-8') 
                await procesar_mensajes(consulta_id, contenido, enviar_mensaje)

        queue = await canal.get_queue(nombre_entrada)
        await queue.consume(wrapper)
        logging.info(f"Escuchando en {nombre_entrada}")

"""