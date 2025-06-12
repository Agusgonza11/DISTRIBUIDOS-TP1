from multiprocessing import Process
import os
import signal
import sys
import pika # type: ignore
import logging
from common.utils import cargar_broker, cargar_eof_a_enviar, create_body, fue_reiniciado, graceful_quit, initialize_log, puede_enviar
from common.health import HealthMonitor

# ----------------------
# ENRUTAMIENTO DE MENSAJE
# ----------------------
COLAS = {
    "filter_request_1": "gateway_output",
    "filter_request_2": "aggregator_request_2",
    "filter_request_3": "broker",
    "filter_request_4": "broker",
    "filter_request_5": "broker",
    "aggregator_request_2": "gateway_output",
    "aggregator_request_3": "gateway_output",
    "aggregator_request_4": "gateway_output",
    "aggregator_request_5": "gateway_output",
    "pnl_request_5": "aggregator_request_5",
    "joiner_request_3": "aggregator_request_3",
    "joiner_request_4": "aggregator_request_4",
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


def config_header(mensaje_original, tipo=None):
    headers = {"Query": mensaje_original['headers'].get("Query"), "ClientID": mensaje_original['headers'].get("ClientID")}
    if tipo != None:
        headers["type"] = tipo
    return pika.BasicProperties(headers=headers)

def obtener_client_id(mensaje):
    return mensaje['headers'].get("ClientID")

def obtener_tipo_mensaje(mensaje):
    return mensaje['headers'].get("type")

def obtener_body(mensaje):
    return mensaje['body'].decode('utf-8')

# ---------------------
# GENERALES
# ---------------------
def run(tipo_nodo, nodo):
    reiniciado = False
    if fue_reiniciado(tipo_nodo):
        print("El nodo fue reiniciado", flush=True)
        reiniciado = True
    proceso_nodo = Process(target=iniciar_nodo, args=(tipo_nodo, nodo, reiniciado))
    monitor = HealthMonitor(tipo_nodo)
    proceso_monitor = Process(target=monitor.run)
    def shutdown_parent_handler(_, __):
        print("Recibida señal en padre, terminando hijos...")
        for p in (proceso_nodo, proceso_monitor):
            if p.is_alive():
                p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_parent_handler)
    signal.signal(signal.SIGTERM, shutdown_parent_handler)


    proceso_nodo.start()
    proceso_monitor.start()
    proceso_nodo.join()
    proceso_monitor.join()



def iniciar_nodo(tipo_nodo, nodo, reiniciado=False):
    initialize_log("INFO")
    nodo = nodo(reiniciado)
    consultas = os.getenv("CONSULTAS", "")
    worker_id = int(os.environ.get("WORKER_ID", 0))
    logging.info(f"Se inicializó el {tipo_nodo}")
    consultas = list(map(int, consultas.split(","))) if consultas else []
    conexion, canal = inicializar_comunicacion()
    graceful_quit(conexion, canal, nodo)
    if tipo_nodo == "broker":
        escuchar_colas_broker(nodo, canal)
    elif tipo_nodo == "joiner":
        escuchar_colas_joiner(nodo, consultas, canal, worker_id)
    elif tipo_nodo == "pnl":
        escuchar_colas_pnl(nodo, consultas, canal, worker_id)
    else:
        escuchar_colas(tipo_nodo, nodo, consultas, canal)


def inicializar_comunicacion():
    parametros = pika.ConnectionParameters(host="rabbitmq")
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.basic_qos(prefetch_count=1)
    return conexion, canal



def enviar_mensaje(canal, routing_key, body, mensaje_original, type=None):
    if puede_enviar(body):
        #logging.info(f"A {routing_key} le voy a enviar: {type}")
        propiedades = config_header(mensaje_original, type)
        canal.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=create_body(body).encode(),
            properties=propiedades
        )




# ---------------------
# ATENDER CONSULTA
# ---------------------

def escuchar_colas(entrada, nodo, consultas, canal):        
    for consulta_id in consultas:
        nombre_entrada = f"{entrada}_request_{consulta_id}"
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
            colas_salida.append(f"joiner_request_{consulta_id}_{joiner_id}")
    for i in range(1, pnl[5] + 1):
        colas_salida.append(f"pnl_request_5_{i}")

    for nombre_salida in colas_salida:
        canal.queue_declare(queue=nombre_salida, durable=True)
        generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)

    canal.start_consuming()


def escuchar_colas_joiner(nodo, consultas, canal, joiner_id):   
    colas_entrada = []
    for consulta_id in consultas:
        colas_entrada.append(f"joiner_request_{consulta_id}")
    for consulta_id in consultas:
        colas_entrada.append(f"joiner_request_{consulta_id}_{joiner_id}")

    for nombre_entrada in colas_entrada:
        canal.queue_declare(queue=nombre_entrada, durable=True)
        nombre_salida = "aggregator_request_3" if "3" in nombre_entrada else "aggregator_request_4"        
        canal.queue_declare(queue=nombre_salida, durable=True)
        generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)

    canal.start_consuming()

def escuchar_colas_pnl(nodo, consultas, canal, pnl_id):   

    nombre_entrada = f"pnl_request_5_{pnl_id}"
    canal.queue_declare(queue=nombre_entrada, durable=True)
    nombre_salida = "aggregator_request_5"
    canal.queue_declare(queue=nombre_salida, durable=True)
    generalizo_callback(nombre_entrada, nombre_salida, canal, nodo)
    canal.start_consuming()

def setup_queue(canal, nombre_queue, ttl):
    canal.queue_declare(
        queue=nombre_queue,
        durable=True,
        arguments={"x-message-ttl": ttl}
    )

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



def restaurar_de_backup(queue_name, procesar_func, ttl, max_msgs=None):
    """
    Consume todos los mensajes de una cola backup y los envía a procesar_func.
    - queue_name: Nombre de la cola backup (str)
    - ttl: entero, TTL opcional de la cola (default 15min)
    - max_msgs: cortar después de N mensajes (útil debug/tests)
    """
    parametros = pika.ConnectionParameters(host="rabbitmq")
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    setup_queue(canal, queue_name, ttl)

    count = 0
    while True:
        method_frame, properties, body = canal.basic_get(queue=queue_name, auto_ack=False)
        if method_frame is None:
            break  

        mensaje = {
            'body': body,
            'headers': properties.headers if properties and properties.headers else {},
            'ack': lambda: canal.basic_ack(delivery_tag=method_frame.delivery_tag)
        }
        try:
            nombre_salida = "aggregator_request_3" if queue_name[-1] == "3" else "aggregator_request_4"        
            procesar_func(canal, nombre_salida, mensaje, enviar_mensaje)
        except Exception as e:
            logging.error(f"Error levantando mensaje de backup: {e}")
            canal.basic_ack(delivery_tag=method_frame.delivery_tag)
        count += 1
        if max_msgs and count >= max_msgs:
            break

    conexion.close()