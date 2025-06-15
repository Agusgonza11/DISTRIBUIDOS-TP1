import logging
import pickle

from common.utils import EOF, borrar_contenido_carpeta, cargar_eofs, concat_data, create_dataframe, obtiene_nombre_contenedor, prepare_data_aggregator_consult_3
from common.communication import obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from common.excepciones import ConsultaInexistente


AGGREGATOR = "aggregator"
TMP_DIR = f"/tmp/{obtiene_nombre_contenedor(AGGREGATOR)}_tmp"

# -----------------------
# Nodo Aggregator
# -----------------------
class AggregatorNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.resultados_health = {}
        self.eof_esperados = {}
        self.health_file = f"{TMP_DIR}/health_file.data"
        self.cargar_estado()

    def eliminar(self, es_global):
        self.resultados_parciales = {}
        self.eof_esperados = {}
        self.resultados_health = {}
        if es_global:
            try:
                borrar_contenido_carpeta(self.health_file)
                logging.info(f"Volumen limpiado por shutdown global")
                print(f"Volumen limpiado por shutdown global", flush=True)
            except Exception as e:
                logging.error(f"Error limpiando volumen en shutdown global: {e}")

    def cargar_estado(self):
        try:
            with open(self.health_file, "rb") as f:
                estado = pickle.load(f)
                self.resultados_health = estado.get("resultados_parciales", {})
                self.eof_esperados = estado.get("eof_esperados", {})
                self.resultados_parciales = {}

                # Recrear los DataFrames desde los datos crudos
                for client_id, consultas in self.resultados_health.items():
                    self.resultados_parciales[client_id] = {}
                    for consulta_id, lista_datos in consultas.items():
                        # Aplicar create_dataframe a cada entrada individual
                        dfs = [create_dataframe(datos) for datos in lista_datos]
                        self.resultados_parciales[client_id][consulta_id] = dfs
        except FileNotFoundError:
            logging.info("No hay estado para cargar")
        except Exception as e:
            print(f"Error al cargar el estado: {e}", flush=True)


    def guardar_estado(self):
        try:
            with open(self.health_file, "wb") as f:
                pickle.dump({
                    "resultados_parciales": self.resultados_health,
                    "eof_esperados": self.eof_esperados
                }, f)
        except Exception as e:
            print(f"Error al guardar el estado: {e}", flush=True)


    def guardar_datos(self, consulta_id, datos, client_id):
        if client_id not in self.resultados_parciales:
            self.resultados_parciales[client_id] = {}
            self.resultados_health[client_id] = {}
            self.eof_esperados[client_id] = {}
            
        if consulta_id not in self.resultados_parciales[client_id]:
            self.resultados_parciales[client_id][consulta_id] = []
            self.resultados_health[client_id][consulta_id] = []
            
        if consulta_id not in self.eof_esperados[client_id]:
            self.eof_esperados[client_id][consulta_id] = cargar_eofs()[consulta_id]

        self.resultados_parciales[client_id][consulta_id].append(create_dataframe(datos))
        self.resultados_health[client_id][consulta_id].append(datos)



    def ejecutar_consulta(self, consulta_id, client_id):
        if client_id not in self.resultados_parciales:
            return False
        
        datos_cliente = self.resultados_parciales[client_id]
        if not datos_cliente:
            return False 
        
        datos = concat_data(datos_cliente[consulta_id])
        
        match consulta_id:
            case 2:
                return self.consulta_2(datos)
            case 3:
                return self.consulta_3(datos)
            case 4:
                return self.consulta_4(datos)
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida {consulta_id}")
                raise ConsultaInexistente(f"Consulta {consulta_id} no encontrada")
    

    def consulta_2(self, datos):
        logging.info("Procesando datos para consulta 2")
        suma_por_pais = {}
        for row in datos:
            country = row.get('country')
            try:
                budget = float(row.get('budget', 0))
            except (ValueError, TypeError):
                budget = 0

            if country not in suma_por_pais:
                suma_por_pais[country] = 0
            suma_por_pais[country] += budget
        top_5 = sorted(suma_por_pais.items(), key=lambda x: x[1], reverse=True)[:5]
        resultado = [{'country': pais, 'budget': suma} for pais, suma in top_5]

        return resultado

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        logging.info(f"Datos recibidos con shape: {datos.shape}")
        
        mean_ratings = datos.groupby(["id", "title"])['rating'].mean().reset_index()
        max_rated = mean_ratings.iloc[mean_ratings['rating'].idxmax()]
        min_rated = mean_ratings.iloc[mean_ratings['rating'].idxmin()]
        result = prepare_data_aggregator_consult_3(min_rated, max_rated)
        return result

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        cast_per_movie_quantities = datos.groupby(["name"]).count().reset_index().rename(columns={"id":"count"})
        top_ten = cast_per_movie_quantities.nlargest(10, 'count')
        return top_ten

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos["rate_revenue_budget"] = datos["revenue"] / datos["budget"]
        average_rate_by_sentiment = datos.groupby("sentiment")["rate_revenue_budget"].mean().reset_index()
        return average_rate_by_sentiment
    

    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        try:
            if tipo_mensaje == EOF:
                logging.info(f"Consulta {consulta_id} de aggregator recibió EOF")
                self.eof_esperados[client_id][consulta_id] -= 1
                if self.eof_esperados[client_id][consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    resultado = self.ejecutar_consulta(consulta_id, client_id)
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    enviar_func(canal, destino, EOF, mensaje, EOF)
                    del self.resultados_parciales[client_id][consulta_id]
                    del self.eof_esperados[client_id][consulta_id]

                    if not self.resultados_parciales[client_id]:
                        del self.resultados_parciales[client_id]
                    if not self.eof_esperados[client_id]:
                        del self.eof_esperados[client_id]
            else:
                self.guardar_datos(consulta_id, obtener_body(mensaje), client_id)
            self.guardar_estado()
            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

if __name__ == "__main__":
    run(AGGREGATOR, AggregatorNode)

    