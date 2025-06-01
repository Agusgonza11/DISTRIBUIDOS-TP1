from multiprocessing import Process
import os
from pathlib import Path
import subprocess
import docker
import socket
import time
import logging
from common.utils import cargar_nodo_siguiente, cargar_nodo_anterior, cargar_puerto, cargar_puerto_siguiente, obtiene_nombre_contenedor


class HealthMonitor:
    def __init__(self, tipo):
        self.puerto_nodo = int(cargar_puerto())
        self.puerto_nodo_siguiente = int(cargar_puerto_siguiente())
        self.nodo_anterior = cargar_nodo_anterior()
        self.nodo_siguiente = cargar_nodo_siguiente()
        self.heartbeat_interval = 2
        self.check_interval = 5
        self.nodo_actual = obtiene_nombre_contenedor(tipo)


    def reinicio(self):
        client = docker.from_env()
        nombre = self.nodo_anterior

        try:
            # Crear el archivo flag de reinicio
            flag_dir = Path("/app/reinicio_flags")
            flag_dir.mkdir(parents=True, exist_ok=True)  
            flag_file = flag_dir / f"{nombre}.flag"
            flag_file.write_text("true") 

            # Reiniciar el contenedor
            container = client.containers.get(nombre)
            container.restart()

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