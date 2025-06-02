from multiprocessing import Process
import os
from pathlib import Path
import docker # type: ignore
import socket
import time
from common.utils import cargar_nodo_siguiente, cargar_nodo_anterior, cargar_puerto, cargar_puerto_siguiente, obtiene_nombre_contenedor
import signal

class HealthMonitor:
    def __init__(self, tipo):
        self.puerto_nodo = int(cargar_puerto())
        self.puerto_nodo_siguiente = int(cargar_puerto_siguiente())
        self.nodo_anterior = cargar_nodo_anterior()
        self.nodo_siguiente = cargar_nodo_siguiente()
        self.nodo_actual = obtiene_nombre_contenedor(tipo)
        self.heartbeat_interval = 4
        self.check_interval = 10
        self.max_failed_heartbeats = 3
        self.failed_heartbeats = 0
        self.running = True
        self.recv = None
        self.send = None

    def reinicio(self):
        client = docker.from_env()
        nombre = self.nodo_anterior

        try:
            flag_dir = Path("/app/reinicio_flags")
            flag_dir.mkdir(parents=True, exist_ok=True)
            flag_file = flag_dir / f"{nombre}.flag"
            flag_file.write_text("true")

            container = client.containers.get(nombre)
            container.restart()

        except Exception as e:
            print(f"Error reiniciando contenedor {nombre}: {e}", flush=True)

    def eliminar(self):
        print("[MONITOR] Cerrando conexiones sockets...", flush=True)
        self.running = False
        if self.recv:
            try:
                self.recv.close()
            except Exception as e:
                print(f"Error cerrando socket recv: {e}", flush=True)
        if self.send:
            try:
                self.send.close()
            except Exception as e:
                print(f"Error cerrando socket send: {e}", flush=True)

    def graceful_quit(self, *args):
        self.eliminar()

    def run(self):
        # Manejar señales para permitir graceful quit
        signal.signal(signal.SIGINT, self.graceful_quit)
        signal.signal(signal.SIGTERM, self.graceful_quit)

        self.recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv.bind((self.nodo_actual, self.puerto_nodo))
        self.recv.setblocking(0)

        self.send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        last = time.time()

        while self.running:
            try:
                self.send.sendto(b"HB", (self.nodo_siguiente, self.puerto_nodo_siguiente))
            except socket.gaierror as e:
                print(f"[MONITOR] Nodo siguiente no disponible: {e}", flush=True)
            except Exception as e:
                print(f"[MONITOR] Error al enviar heartbeat: {e}")

            try:
                data, _ = self.recv.recvfrom(1024)
                if data == b"HB":
                    last = time.time()
            except BlockingIOError:
                pass

            if time.time() - last > self.check_interval:
                print("[MONITOR] No se recibio heartbeat", flush=True)
                self.failed_heartbeats += 1
                last = time.time()
                if self.failed_heartbeats >= self.max_failed_heartbeats:
                    print("[MONITOR] No se recibieron heartbeats suficientes.", flush=True)
                    self.failed_heartbeats = 0
                    self.reinicio()

            time.sleep(self.heartbeat_interval)

        print("[MONITOR] Loop principal terminado.", flush=True)
