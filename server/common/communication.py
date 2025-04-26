import pika # type: ignore
import logging
from common.utils import create_body, initialize_log, puede_enviar


# ----------------------
# Constantes globales
# ----------------------
conexion = None
canal = None

# ----------------------
# ENRUTAMIENTO DE MENSAJE
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


QUERY = {
    "ARGENTINIAN-SPANISH-PRODUCTIONS": 1,
    "TOP-INVESTING-COUNTRIES" : 2,
    "TOP-ARGENTINIAN-MOVIES-BY-RATING": 3,
    "TOP-ARGENTINIAN-ACTORS": 4,
    "SENTIMENT-ANALYSIS": 5,
}


def obtener_query(mensaje):
    tipo = mensaje['headers'].get("Query")
    return QUERY[tipo]


#Utilizar a futuro para colas unicas
def determinar_salida(tipo_nodo, consulta_id):
    if tipo_nodo == "filter":
        return {
            1: "gateway_output",
            2: "aggregator_input",
            3: "joiner_input",
            4: "joiner_input",
            5: "pnl_input"
        }[consulta_id]
    elif tipo_nodo == "joiner":
        return {
            3: "aggregator_input",
            4: "aggregator_input"
        }[consulta_id]
    elif tipo_nodo == "aggregator":
        return {
            2: "gateway_output",
            3: "gateway_output",
            4: "gateway_output",
            5: "gateway_output"
        }[consulta_id]
    elif tipo_nodo == "pnl":
        return {
            5: "aggregator_input"
        }[consulta_id]
    else:
        raise ValueError(f"No se puede determinar salida para {tipo_nodo} y consulta {consulta_id}")


def config_header(mensaje_original, tipo=None):
    headers = {"Query": mensaje_original['headers'].get("Query"), "ClientID": mensaje_original['headers'].get("ClientID")}
    if type != None:
        headers["type"] = tipo
    return pika.BasicProperties(headers=headers)  



# ---------------------
# GENERALES
# ---------------------
def iniciar_nodo(tipo_nodo, nodo, consultas):
    initialize_log("INFO")
    logging.info(f"Se inicializó el {tipo_nodo} filter")
    consultas = list(map(int, consultas.split(","))) if consultas else []
    inicializar_comunicacion()
    escuchar_colas(tipo_nodo, nodo, consultas)
    nodo.shutdown_event.wait()
    logging.info(f"Shutdown del nodo {tipo_nodo}")

def inicializar_comunicacion():
    global conexion, canal
    parametros = pika.ConnectionParameters(host="rabbitmq")
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.basic_qos(prefetch_count=1)


def enviar_mensaje(routing_key, body, mensaje_original, type=None):
    if puede_enviar(body):
        logging.info(f"Lo que voy a enviar es {body}")
        propiedades = config_header(mensaje_original, type)
        canal.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=create_body(body).encode(),
            properties=propiedades
        )
    #else:
        #logging.info("No se enviará el mensaje: body vacío")


# ---------------------
# ATENDER CONSULTA
# ---------------------

def escuchar_colas(entrada, nodo, consultas):
    for consulta_id in consultas:
        nombre_entrada = f"{entrada}_consult_{consulta_id}"
        nombre_salida = COLAS[nombre_entrada]

        canal.queue_declare(queue=nombre_entrada, durable=True)
        canal.queue_declare(queue=nombre_salida, durable=True)


        def make_callback(nombre_salida):
            def callback(ch, method, properties, body):
                mensaje = {
                    'body': body,
                    'headers': properties.headers if properties.headers else {},
                    'ack': lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                }
                nodo.procesar_mensajes(nombre_salida, mensaje, enviar_mensaje)
            return callback

        canal.basic_consume(
            queue=nombre_entrada,
            on_message_callback=make_callback(nombre_salida),
            auto_ack=False
        )

        logging.info(f"Escuchando en {nombre_entrada}")
        logging.info(f"Para enviar en {nombre_salida}")

    canal.start_consuming()

