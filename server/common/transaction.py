import os
import pickle
import threading

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

COMMITS = {
    RESULT: "/M",
    EOF_ESPERADOS: "/E",
    ULTIMO_NODO: "/U",
    NODOS_ENVIAR: "/N",
    EOF_ESPERA: "/S",
    CLIENTS: "/C",
    DATA: "/D",
    TERM: "/T",
    DISK: "/K",
    PATHS: "/P"
}

SUFIJOS_A_CLAVE = {v: k for k, v in COMMITS.items()}


class Transaction:
    def __init__(self, base_dir, modificaciones):
        self.directorio = base_dir
        os.makedirs(self.directorio, exist_ok=True)
        self.archivo = os.path.join(self.directorio, ".data")
        self.archivos = {
            clave: os.path.join(self.directorio, f"{clave}.data")
            for clave in modificaciones
        }
        self.cambios = {clave: False for clave in modificaciones}

    def borrar_carpeta(self):
        for archivo in self.archivos.values():
            if os.path.exists(archivo):
                os.remove(archivo)
        if os.path.exists(self.archivo):
            os.remove(self.archivo)


    def marcar_modificado(self, claves):
        for clave in claves:
            self.cambios[clave] = True


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






    def commit(self, claves, valores):
        for clave in claves:
            self._commit(clave, valores)


    def _commit(self, clave, valores):
        sufijo = COMMITS.get(clave)
        if sufijo is None:
            raise ValueError(f"Clave '{clave}' no encontrada en COMMITS")
        try:
            partes = [str(v) if not isinstance(v, (dict, list)) else repr(v) for v in valores]
            contenido = "|".join(partes)
            commit = f"{contenido}{sufijo}\n"

            with open(self.archivo, "a") as f:
                f.write(commit)
                f.flush()
        except Exception as e:
            raise Exception(f"Error serializando {clave}: {e}")


    def _cargar_estado(self, nodo):
        try:
            with open(self.archivo, "r") as f:
                for linea in f:
                    linea = linea.strip()
                    sufijo = linea[-2:]
                    clave = SUFIJOS_A_CLAVE[sufijo]
                    contenido = linea[:-2]
                    nodo.reconstruir(clave, contenido)
        except FileNotFoundError:
            print(f"No hay estado para guardar", flush=True)