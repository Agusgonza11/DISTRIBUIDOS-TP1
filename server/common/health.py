import os
from common.utils import cargar_nodo_siguiente
from common.communication import inicializar_comunicacion

class HealthMonitor:
    def __init__(self):
        self.nodo_siguiente = cargar_nodo_siguiente()
        self.conexion, self.canal = inicializar_comunicacion()
        self.canal.queue_declare(queue=self.nodo_siguiente, durable=True)
        print(f"HealthMonitor listo. Nodo siguiente: {self.nodo_siguiente}")
        
    def enviar_mensaje(self, mensaje, cola=None):
        cola_destino = cola if cola else os.getenv("NODO_ACTUAL", "desconocido")
        self.canal.basic_publish(
            exchange='',
            routing_key=cola_destino,
            body=mensaje.encode() if isinstance(mensaje, str) else mensaje
        )
        print(f"Mensaje enviado a la cola {cola_destino}: {mensaje}")



    def run(self, tipo):
        def callback(ch, method, properties, body):
            print(f"Mensaje recibido: {body}")
            self.canal.basic_publish(exchange='', routing_key=self.nodo_siguiente, body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        if tipo != "broker":
            worker_id = int(os.environ.get("WORKER_ID", 0))
            nodo_actual = f"{tipo}{worker_id}"
        else:
            nodo_actual = "broker"
        self.canal.queue_declare(queue=nodo_actual, durable=True)
        self.canal.basic_consume(queue=nodo_actual, on_message_callback=callback)
        print(f"HealthMonitor escuchando en cola: {nodo_actual}")
        self.canal.start_consuming()
