from multiprocessing import Process
import os
import select
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
        print("se cayo el nodo", flush=True)
        pass

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
                print("envia heartbeat", flush=True)
            except Exception as e:
                logging.info("entra aca") 
                print(f"[MONITOR] Error al enviar heartbeat: {e}")

            try:
                data, _ = recv.recvfrom(1024)
                if data == b"HB":
                    print("recibi heartbeat", flush=True)
                    last = time.time()
            except BlockingIOError:
                pass

            if time.time() - last > self.check_interval:
                print("no se recibio mas heartbeat", flush=True)
                self.reinicio()

            time.sleep(self.heartbeat_interval)