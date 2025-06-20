import os

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

PNL = "pnl"


COMMITS = {
    RESULT: "/M",
    EOF_ESPERADOS: "/E",

    ULTIMO_NODO: "/U",
    EOF_ESPERA: "/S",

    DATA: "/D",
    TERM: "/T",
    DATA_TERM: "/Y",
    DISK: "/K",
}

SUFIJOS_A_CLAVE = {v: k for k, v in COMMITS.items()}


class Transaction:
    def __init__(self, base_dir):
        self.directorio = base_dir
        os.makedirs(self.directorio, exist_ok=True)
        self.archivo = os.path.join(self.directorio, ".data")

    def borrar_carpeta(self):
        if os.path.exists(self.archivo):
            os.remove(self.archivo)


    def commit(self, clave, valores):
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


    def cargar_estado(self, nodo):
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