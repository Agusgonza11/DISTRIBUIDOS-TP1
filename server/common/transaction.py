import logging
import pickle
from common.utils import borrar_contenido_carpeta, create_dataframe
#from workers.broker import CLIENTS, EOF_ESPERA, NODOS_ENVIAR, ULTIMO_NODO


class Transaction:
    def __init__(self, health_file_path, modificaciones):
        self.health_file = health_file_path
        self.cambios = {clave: False for clave in modificaciones}

    def borrar_carpeta(self):
        borrar_contenido_carpeta(self.health_file)

    def marcar_modificado(self, claves):
        for clave in claves:
            self.cambios[clave] = True

    def marcar_no_modificado(self, claves):
        for clave in claves:
            self.cambios[clave] = False




    def guardar_estado_aggregator(self, agg):
        try:
            with open(self.health_file, "wb") as f:
                pickle.dump({
                    "resultados_parciales": agg.resultados_health,
                    "eof_esperados": agg.eof_esperados
                }, f)
        except Exception as e:
            print(f"Error al guardar el estado: {e}", flush=True)


    def cargar_estado_aggregator(self, agg):
        def clean_csv_data(raw_csv_str, expected_header="production_countries,budget,country"):
            lines = raw_csv_str.splitlines()
            filtered_lines = [line for line in lines if line.strip() != expected_header]
            cleaned_csv = "\n".join(filtered_lines)
            return cleaned_csv

        try:
            with open(self.health_file, "rb") as f:
                estado = pickle.load(f)
                agg.resultados_health = estado.get("resultados_parciales", {})
                agg.eof_esperados = estado.get("eof_esperados", {})
                agg.resultados_parciales = {}

                # Recrear los DataFrames desde los datos crudos, limpiando encabezados intercalados
                for client_id, consultas in agg.resultados_health.items():
                    agg.resultados_parciales[client_id] = {}
                    for consulta_id, lista_datos in consultas.items():
                        dfs = []
                        for datos in lista_datos:
                            cleaned = clean_csv_data(datos)
                            df = create_dataframe(cleaned)
                            dfs.append(df)
                        agg.resultados_parciales[client_id][consulta_id] = dfs

        except FileNotFoundError:
            logging.info("No hay estado para cargar")
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)






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