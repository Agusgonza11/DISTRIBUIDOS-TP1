import os

NO_ENVIAR = "no_enviar"
ENVIAR = "enviar"

AGGREGATOR = "aggregator"
RESULT = "resultados_parciales"
EOF_ESPERADOS = "eof_esperados"

BROKER = "broker"
ULTIMO_NODO = "ultimo_nodo_consulta"
EOF_ESPERA = "eof_esperar"

JOINER = "joiner"
DATA = "datos"
TERM = "termino_movies"
DATA_TERM = "termino_datos"
DISK = "files_on_disk"
PATH = "file_paths"

PNL = "pnl"

ACCION = "accion"

COMMITS = {
    RESULT: "/M",
    EOF_ESPERADOS: "/E",

    ULTIMO_NODO: "/U",
    EOF_ESPERA: "/S",

    DATA: "/D",
    TERM: "/T",
    DATA_TERM: "/Y",
    DISK: "/K",
    PATH: "/P",

    ACCION: "/C"
}

SUFIJOS_A_CLAVE = {v: k for k, v in COMMITS.items()}

def parse_linea(linea):
    linea = linea.strip()
    sufijo = linea[-2:]
    clave = SUFIJOS_A_CLAVE[sufijo]
    contenido = linea[:-2]
    return clave, contenido


class Transaction:
    def __init__(self, base_dir):
        self.directorio = base_dir
        os.makedirs(self.directorio, exist_ok=True)
        self.archivo = os.path.join(self.directorio, ".data")
        self.archivo_acciones = os.path.join(self.directorio, "-acciones.data")
        self.ultima_accion = []

    def borrar_carpeta(self):
        if os.path.exists(self.archivo):
            os.remove(self.archivo)

    def cargar_ultima_accion(self, contenido):
        batch_id, resultado, accion = contenido.split("|", 2)
        self.ultima_accion = [batch_id, accion, resultado]

    def comprobar_ultima_accion(self, client_id, batch_id, enviar_func, mensaje, canal, destino):
        # a esta funcion tienen que llamar todos los nodos cuando reciben mensaje
        if self.ultima_accion[0] == batch_id:
            if self.ultima_accion[1] == ENVIAR:
                resultado = self.ultima_accion[2]
                self.commit(ACCION, [destino, mensaje, client_id, batch_id, resultado, ENVIAR])
                tipo = "EOF" if resultado == "EOF" else "RESULT"
                enviar_func(canal, destino, resultado, mensaje, tipo)
                self.commit(ACCION, ["", "", client_id, batch_id, "", NO_ENVIAR])
            mensaje['ack']()
            return True
        return False




    def commit(self, clave, valores):
        sufijo = COMMITS.get(clave)
        if sufijo is None:
            raise ValueError(f"Clave '{clave}' no encontrada en COMMITS")
        try:
            partes = [str(v) if not isinstance(v, (dict, list)) else repr(v) for v in valores]
            contenido = "|".join(partes)
            commit = f"{contenido}{sufijo}\n"
            if clave == ACCION:
                ruta = self.archivo_acciones
                modo = "w"
            else:
                ruta = self.archivo
                modo = "a"
            with open(ruta, modo) as f:
                f.write(commit)
                f.flush()
        except Exception as e:
            raise Exception(f"Error serializando {clave}: {e}")


    def cargar_estado(self, nodo):
        try:
            with open(self.archivo, "r") as f:
                for linea in f:
                    clave, contenido = parse_linea(linea)
                    nodo.reconstruir(clave, contenido)
            with open(self.archivo_acciones, "r") as f:
                for linea in f:
                    _, contenido = parse_linea(linea)
                    self.cargar_ultima_accion(contenido)
                return True
        except FileNotFoundError:
            print(f"No hay estado para guardar", flush=True)
            return False