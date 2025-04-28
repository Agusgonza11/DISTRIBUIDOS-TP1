import pika # type: ignore
import logging
from common.utils import cargar_broker, cargar_eof_a_enviar, create_body, initialize_log, puede_enviar


# ----------------------
# Constantes globales
# ----------------------

# ----------------------
# ENRUTAMIENTO DE MENSAJE
# ----------------------
COLAS = {
    "filter_consult_1": "gateway_output",
    "filter_consult_2": "aggregator_consult_2",
    "filter_consult_3": "broker",
    "filter_consult_4": "broker",
    "filter_consult_5": "broker",
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


#Utilizar para colas unicas
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
def iniciar_nodo(tipo_nodo, nodo, consultas=None, worker_id=None):
    initialize_log("INFO")
    logging.info(f"Se inicializó el {tipo_nodo} filter")
    consultas = list(map(int, consultas.split(","))) if consultas else []
    _, canal = inicializar_comunicacion()
    if tipo_nodo == "broker":
        escuchar_colas_broker(nodo, canal)
    elif tipo_nodo == "joiner":
        escuchar_colas_joiner(nodo, consultas, canal, worker_id)
    elif tipo_nodo == "pnl":
        escuchar_colas_pnl(nodo, consultas, canal, worker_id)
    else:
        escuchar_colas(tipo_nodo, nodo, consultas, canal)
    nodo.shutdown_event.wait()
    logging.info(f"Shutdown del nodo {tipo_nodo}")


def inicializar_comunicacion():
    parametros = pika.ConnectionParameters(host="rabbitmq")
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.basic_qos(prefetch_count=1)
    return conexion, canal


def enviar_mensaje(canal, routing_key, body, mensaje_original, type=None):
    if puede_enviar(body):
        #logging.info(f"A {routing_key} le voy a enviar: {body}")
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

def escuchar_colas(entrada, nodo, consultas, canal):        
    for consulta_id in consultas:
        nombre_entrada = f"{entrada}_consult_{consulta_id}"
        nombre_salida = COLAS[nombre_entrada]

        canal.queue_declare(queue=nombre_entrada, durable=True)
        canal.queue_declare(queue=nombre_salida, durable=True)

        generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)

    canal.start_consuming()



def escuchar_colas_broker(nodo, canal): 
    joiners = cargar_broker()
    pnl = cargar_eof_a_enviar()
    nombre_entrada = "broker"
    canal.queue_declare(queue=nombre_entrada, durable=True)
    colas_salida = []
    for joiner_id, consultas in joiners.items():
        for consulta_id in consultas:
            colas_salida.append(f"joiner_consult_{consulta_id}_{joiner_id}")
    for i in range(1, pnl[5] + 1):
        colas_salida.append(f"pnl_consult_5_{i}")

    for nombre_salida in colas_salida:
        canal.queue_declare(queue=nombre_salida, durable=True)
        generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)

    canal.start_consuming()


def escuchar_colas_joiner(nodo, consultas, canal, joiner_id):   
    colas_entrada = []
    for consulta_id in consultas:
        colas_entrada.append(f"joiner_consult_{consulta_id}")
    for consulta_id in consultas:
        colas_entrada.append(f"joiner_consult_{consulta_id}_{joiner_id}")

    for nombre_entrada in colas_entrada:
        canal.queue_declare(queue=nombre_entrada, durable=True)
        nombre_salida = "aggregator_consult_3" if "3" in nombre_entrada else "aggregator_consult_4"        
        canal.queue_declare(queue=nombre_salida, durable=True)
        generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)

    canal.start_consuming()

def escuchar_colas_pnl(nodo, consultas, canal, pnl_id):   

    nombre_entrada = f"pnl_consult_5_{pnl_id}"
    canal.queue_declare(queue=nombre_entrada, durable=True)
    nombre_salida = "aggregator_consult_5"
    canal.queue_declare(queue=nombre_salida, durable=True)
    generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)
    canal.start_consuming()


def generalizo_callback(nombre_entrada, nombre_salida, canal, nodo):
    def make_callback(nombre_salida):
        def callback(ch, method, properties, body):
            mensaje = {
                'body': body,
                'headers': properties.headers if properties.headers else {},
                'ack': lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            }
            nodo.procesar_mensajes(canal, nombre_salida, mensaje, enviar_mensaje)
        return callback

    canal.basic_consume(
        queue=nombre_entrada,
        on_message_callback=make_callback(nombre_salida),
        auto_ack=False
    )

    logging.info(f"Escuchando en {nombre_entrada}")
    logging.info(f"Para enviar en {nombre_salida}")    