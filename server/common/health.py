from multiprocessing import Process
import os
import subprocess
import docker
import socket
import time
import logging
from common.utils import cargar_nodo_siguiente, cargar_nodo_anterior, cargar_puerto, cargar_puerto_siguiente


class HealthMonitor:
    def __init__(self, tipo):
        self.puerto_nodo = int(cargar_puerto())
        self.puerto_nodo_siguiente = int(cargar_puerto_siguiente())
        self.nodo_anterior = cargar_nodo_anterior()
        self.nodo_siguiente = cargar_nodo_siguiente()
        self.heartbeat_interval = 2
        self.check_interval = 5
        if tipo != "broker":
            worker_id = int(os.environ.get("WORKER_ID", 0))
            self.nodo_actual = f"{tipo}{worker_id}"
        else:
            self.nodo_actual = "broker"


    def reinicio(self):
        client = docker.from_env()
        nombre = self.nodo_anterior  # ej: "filter2"

        try:
            container = client.containers.get(nombre)
            # Usar restart que detiene y arranca el mismo contenedor (sin recrear)
            container.restart()
            print(f"Contenedor {nombre} reiniciado", flush=True)
        except Exception as e:
            print(f"Error reiniciando contenedor {nombre}: {e}", flush=True)


    def run(self):
        recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv.bind((self.nodo_actual, self.puerto_nodo))
        recv.setblocking(0)

        send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        logging.info("3") 

        last = time.time()
        while True:
            try:
                send.sendto(b"HB", (self.nodo_siguiente, self.puerto_nodo_siguiente))
            except Exception as e:
                print(f"[MONITOR] Error al enviar heartbeat: {e}")

            try:
                data, _ = recv.recvfrom(1024)
                if data == b"HB":
                    last = time.time()
            except BlockingIOError:
                pass

            if time.time() - last > self.check_interval:
                print("no se recibio mas heartbeat", flush=True)
                self.reinicio()

            time.sleep(self.heartbeat_interval)