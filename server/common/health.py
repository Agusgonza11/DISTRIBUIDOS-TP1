from multiprocessing import Process
import os
from common.utils import cargar_nodo_siguiente

PUERTO1 = 6000
PUERTO2 = 6001

class HealthMonitor:
    def __init__(self, tipo):
        """
        self.nodo_siguiente = cargar_nodo_siguiente()
        if tipo != "broker":
            worker_id = int(os.environ.get("WORKER_ID", 0))
            self.nodo_actual = f"{tipo}{worker_id}"
        else:
            self.nodo_actual = "broker"
        """
        worker_id = int(os.environ.get("WORKER_ID", 0))
        if worker_id == 1:
            self.nodo_actual = PUERTO1
            self.nodo_siguiente = PUERTO2
        else:
            self.nodo_actual = PUERTO2
            self.nodo_siguiente = PUERTO1
        
        
        
    def run(self):
        pass