import os
import pickle
import threading

from common.utils import EOF

ENVIAR = "ENVIAR"
NO_ENVIAR = "NO ENVIAR"

BATCH_ID = "batch_id"

AGGREGATOR = "aggregator"
RESULT = "resultados_parciales"
EOF_ESPERADOS = "eof_esperados"

BROKER = "broker"
ULTIMO_NODO = "ultimo_nodo_consulta"
NODOS_ENVIAR = "nodos_enviar"
EOF_ESPERA = "eof_esperar"
CLIENTS = "clients"

JOINER = "joiner"
DATA = "datos"
TERM = "termino_movies"
DISK = "files_on_disk"
PATHS = "file_paths"

PNL = "pnl"
LINEAS = "lineas_actuales"



class Transaction:
    def __init__(self, base_dir, modificaciones):
        self.directorio = base_dir
        os.makedirs(self.directorio, exist_ok=True)

        self.archivos = {
            clave: os.path.join(self.directorio, f"{clave}.data")
            for clave in modificaciones
        }
        self.cambios = {clave: False for clave in modificaciones}

    def borrar_carpeta(self):
        for archivo in self.archivos.values():
            if os.path.exists(archivo):
                os.remove(archivo)

    def marcar_modificado(self, claves):
        for clave in claves:
            self.cambios[clave] = True

    def actualizar_estado(self, nodo, client_id, batch_id, resultado=None):
        instruccion = NO_ENVIAR if resultado == None else ENVIAR
        nodo.ultimo_mensaje[client_id] = [batch_id, resultado, instruccion]
        self.marcar_modificado([BATCH_ID])
        self.guardar_estado(nodo)


    def comprobar(self, nodo, client_id, batch_id, enviar_func, mensaje, canal, destino):
        if client_id in nodo.ultimo_mensaje:
            if nodo.ultimo_mensaje[client_id][0] == batch_id:
                instruccion = nodo.ultimo_mensaje[client_id][2]
                if instruccion == ENVIAR:
                    resultado = nodo.ultimo_mensaje[client_id][1]
                    tipo = "RESULT" if resultado == EOF else EOF
                    enviar_func(canal, destino, resultado, mensaje, tipo)
                mensaje['ack']()
                return True
        return False





    def estado_aggregator(self, datos, clave, agg):
        if clave == RESULT:
            agg.resultados_parciales = datos
        elif clave == EOF_ESPERADOS:
            agg.eof_esperados = datos

    def estado_broker(self, datos, clave, broker):
        if clave == NODOS_ENVIAR:
            broker.nodos_enviar = datos
        if clave == EOF_ESPERA:
            broker.eof_esperar = datos
        if clave == ULTIMO_NODO:
            broker.ultimo_nodo_consulta = datos
        if clave == CLIENTS:
            broker.clients = datos
    
    def estado_pnl(self, datos, clave, broker):
        if clave == RESULT:
            broker.resultados_parciales = datos
        if clave == LINEAS:
            broker.lineas_actuales = datos

    def estado_joiner(self, datos, clave, joiner):
        if clave == DATA:
            joiner.datos = datos
        if clave == TERM:
            joiner.termino_movies = datos
            joiner.locks = {
                client_id: {
                    "ratings": threading.Lock(),
                    "credits": threading.Lock()
                } for client_id in datos
            }
        if clave == RESULT:
            joiner.resultados_parciales = datos
        if clave == DISK:
            joiner.files_on_disk = datos
        if clave == PATHS:
            joiner.file_paths = datos





    def guardar_estado(self, nodo):
        datos_dict = nodo.estado_a_guardar()
        for clave, datos in datos_dict.items():
            if self.cambios.get(clave, False):
                try:
                    with open(self.archivos[clave], "wb") as f:
                        pickle.dump(datos, f)
                        f.flush()
                    self.cambios[clave] = False
                except Exception as e:
                    print(f"[Transaction] Error al guardar {clave}: {e}", flush=True)


    def cargar_estado(self, nodo, tipo):
        for clave, ruta in self.archivos.items():
            try:
                with open(ruta, "rb") as f:
                    datos = pickle.load(f)
                    if tipo == AGGREGATOR:
                        self.estado_aggregator(datos, clave, nodo)
                    if tipo == BROKER:
                        self.estado_broker(datos, clave, nodo)
                    if tipo == PNL:
                        self.estado_pnl(datos, clave, nodo)
                    if tipo == JOINER:
                        self.estado_joiner(datos, clave, nodo)
            except FileNotFoundError:
                print(f"No hay estado para guardar", flush=True)
            except Exception as e:
                print(f"[Transaction] Error al cargar {clave}: {e}", flush=True)



