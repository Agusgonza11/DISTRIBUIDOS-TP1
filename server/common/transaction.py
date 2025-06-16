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



class Transaction:
    def __init__(self, base_dir, modificaciones):
        self.directorio = base_dir
        os.makedirs(self.directorio, exist_ok=True)

        self.archivos = {
            clave: os.path.join(self.directorio, f"{clave}.data")
            for clave in modificaciones
        }
        self.cambios = {clave: False for clave in modificaciones}
        self.x = 0

    def borrar_carpeta(self):
        for archivo in self.archivos.values():
            if os.path.exists(archivo):
                os.remove(archivo)

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





    def guardar_agg(self, datos, archivo):
        if any(isinstance(v, dict) and isinstance(next(iter(v.values())), list) for v in datos.values()):
            texto = self.serializar_resultados_parciales(datos)
        else:
            texto = self.serializar_eof_esperados(datos)
        archivo.write(texto.encode('utf-8'))
        archivo.flush()







    def guardar_estado(self, nodo, tipo=None):
        datos_dict = nodo.estado_a_guardar()
        for clave, datos in datos_dict.items():
            if self.cambios.get(clave, False):
                try:
                    with open(self.archivos[clave], "wb") as f:
                        if tipo == AGGREGATOR:
                            self.guardar_agg(datos, f)
                        else:
                            pickle.dump(datos, f)
                            f.flush()
                    self.cambios[clave] = False
                except Exception as e:
                    print(f"[Transaction] Error al guardar {clave}: {e}", flush=True)


    def cargar_estado(self, nodo, tipo):
        for clave, ruta in self.archivos.items():
            try:
                with open(ruta, "rb") as f:
                    if tipo == AGGREGATOR:
                        texto = f.read().decode("utf-8")
                        datos = self.deserializar_archivo_por_clave(clave, texto)
                        self.estado_aggregator(datos, clave, nodo)
                        continue
                    datos = pickle.load(f)
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




# -------------------
# SERIALIZACION
# -------------------

    def serializar_resultados_parciales(self, datos: dict) -> str:
        lineas = []
        for client_id, consultas in datos.items():
            for consulta_id, batches in consultas.items():
                for batch in batches:
                    for row in batch:
                        fila = [
                            client_id,
                            str(consulta_id),
                            row['production_countries'],
                            row['budget'],
                            row['country']
                        ]
                        lineas.append('|'.join(fila))
        return '\n'.join(lineas)

    def serializar_eof_esperados(self, datos: dict) -> str:
        lineas = []
        for client_id, consultas in datos.items():
            for consulta_id, valor in consultas.items():
                fila = [client_id, str(consulta_id), str(valor)]
                lineas.append('|'.join(fila))
        return '\n'.join(lineas)


# -------------------
# DESERIALIZACION
# -------------------

    def deserializar_archivo_por_clave(self, clave, texto):
        if clave == RESULT:
            return self.deserializar_resultados_parciales(texto)
        elif clave == EOF_ESPERADOS:
            return self.deserializar_eof_esperados(texto)
        else:
            raise ValueError(f"No se reconoce cómo deserializar la clave {clave}")


    def deserializar_eof_esperados(self, texto: str) -> dict:
        resultado = {}
        for linea in texto.strip().splitlines():
            client_id, consulta_id, valor = linea.strip().split('|')
            consulta_id = int(consulta_id)
            valor = int(valor)
            resultado.setdefault(client_id, {})[consulta_id] = valor
        return resultado


    def deserializar_resultados_parciales(self, texto: str) -> dict:
        resultado = {}
        for linea in texto.strip().splitlines():
            print(f"la linea es {linea}",flush=True)
            client_id, consulta_id, prod, budget, country = linea.strip().split('|')
            consulta_id = int(consulta_id)
            resultado.setdefault(client_id, {}).setdefault(consulta_id, []).append({
                'production_countries': prod,
                'budget': budget,
                'country': country
            })

        for client_id in resultado:
            for consulta_id in resultado[client_id]:
                resultado[client_id][consulta_id] = [resultado[client_id][consulta_id]]
        print(f"el resultado es {resultado}",flush=True)
        return resultado
