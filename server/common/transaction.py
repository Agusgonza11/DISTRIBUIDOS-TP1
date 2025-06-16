import logging
import os
import pickle
from common.utils import create_dataframe



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

    def marcar_no_modificado(self, claves):
        for clave in claves:
            self.cambios[clave] = False




    def estado_aggregator(self, datos, clave, agg):
        if clave == "resultados_parciales":
            print(f"lo que cargue es {len(datos)}",flush=True)
            agg.resultados_parciales = datos
        elif clave == "eof_esperados":
            agg.eof_esperados = datos





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
                    if tipo == "aggregator":
                        self.estado_aggregator(datos, clave, nodo)
            except FileNotFoundError:
                print(f"No hay estado para guardar", flush=True)
            except Exception as e:
                print(f"[Transaction] Error al cargar {clave}: {e}", flush=True)




    def cargar_estado_pnl(self, pnl):
        try:
            with open(self.health_file, "rb") as f:
                estado = pickle.load(f)
                pnl.resultados_health = estado.get("resultados_parciales", {})
                pnl.lineas_actuales = estado.get("lineas_actuales", {})
                pnl.resultados_parciales = {}

                # Recrear los DataFrames desde los datos crudos
                for client_id, lista_datos in pnl.resultados_health.items():
                    pnl.resultados_parciales[client_id] = [
                        create_dataframe(datos) for datos in lista_datos
                    ]
        except FileNotFoundError:
            logging.info("No hay estado para cargar")
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)


    def guardar_estado_pnl(self, result, lineas):
        if not self.cambios["RESULT"]:
            return
        try:
            with open(self.health_file, "wb") as f:
                pickle.dump({
                    "resultados_parciales": result,
                    "lineas_actuales": lineas
                }, f)
        except Exception as e:
            print(f"Error al guardar el estado: {e}", flush=True)





    def guardar_estado_broker(self, broker):
        #if not self.modifico:
        #    return
        try:
            with open(self.health_file, "wb") as f:
                pickle.dump(broker.cambios, f)

            self.modifico = False
        except Exception as e:
            print(f"Error al guardar el estado del broker: {e}", flush=True)

    def cargar_estado_broker(self, broker):
        try:
            with open(self.health_file, "rb") as f:
                cambios = pickle.load(f)

            if "nodos_enviar" in cambios:
                broker.nodos_enviar = pickle.loads(cambios["nodos_enviar"])
            if "eof_esperar" in cambios:
                broker.eof_esperar = pickle.loads(cambios["eof_esperar"])
            if "ultimo_nodo_consulta" in cambios:
                broker.ultimo_nodo_consulta = pickle.loads(cambios["ultimo_nodo_consulta"])
            if "clients" in cambios:
                broker.clients = pickle.loads(cambios["clients"])
            #self.marcar_modificado([ULTIMO_NODO, NODOS_ENVIAR, EOF_ESPERA, CLIENTS])


        except FileNotFoundError:
            logging.info("No hay estado para cargar")
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)