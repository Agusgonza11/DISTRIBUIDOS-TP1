from ast import literal_eval
import logging
from collections import Counter, defaultdict

from common.utils import EOF, cargar_eofs, concat_data, create_dataframe, obtener_message_id, obtiene_nombre_contenedor, parse_datos, prepare_data_aggregator_consult_3
from common.communication import obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from common.excepciones import ConsultaInexistente, ErrorCargaDelEstado
from common.transaction import Transaction

AGGREGATOR = "aggregator"
TMP_DIR = f"/tmp/{obtiene_nombre_contenedor(AGGREGATOR)}_tmp"
RESULT = "resultados_parciales"
EOF_ESPERADOS = "eof_esperados"
ACCION = "accion"

NO_ENVIAR = "no_enviar"
ENVIAR = "enviar"

# -----------------------
# Nodo Aggregator
# -----------------------
class AggregatorNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.eof_esperados = {}
        self.transaction = Transaction(TMP_DIR)
        self.transaction.cargar_estado(self)
    
    def reconstruir(self, clave, contenido):
        client_id, consulta_id, valor = contenido.split("|", 2)
        match clave:
            case "resultados_parciales":
                if client_id not in self.resultados_parciales:
                    self.resultados_parciales[client_id] = {}
                consulta_id = int(consulta_id)
                if consulta_id not in self.resultados_parciales[client_id]:
                    self.resultados_parciales[client_id][consulta_id] = []
                self.resultados_parciales[client_id][consulta_id].append(parse_datos(valor))
            case "eof_esperados":
                if client_id not in self.eof_esperados:
                    self.eof_esperados[client_id] = {}
                self.eof_esperados[client_id][consulta_id] = int(valor)
            case _:
                raise ErrorCargaDelEstado(f"Error en la carga del estado")

    def eliminar(self, es_global):
        self.resultados_parciales.clear()
        self.eof_esperados.clear()
        if es_global:
            try:
                self.transaction.borrar_carpeta()
                logging.info(f"Volumen limpiado por shutdown global")
            except Exception as e:
                logging.error(f"Error limpiando volumen en shutdown global: {e}")



    def guardar_datos(self, consulta_id, datos, client_id):
        if client_id not in self.resultados_parciales:
            self.resultados_parciales[client_id] = {}
            self.eof_esperados[client_id] = {}
            
        if consulta_id not in self.resultados_parciales[client_id]:
            self.resultados_parciales[client_id][consulta_id] = []
            
        if consulta_id not in self.eof_esperados[client_id]:
            self.eof_esperados[client_id][consulta_id] = cargar_eofs()[consulta_id]
            self.transaction.commit(EOF_ESPERADOS, [client_id, consulta_id, self.eof_esperados[client_id][consulta_id]])
        datos = create_dataframe(datos)
        self.resultados_parciales[client_id][consulta_id].append(datos)
        self.transaction.commit(RESULT, [client_id, consulta_id, datos])



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
        if not datos:
            return None
        agrupados = {}
        for d in datos:
            key = (d["id"], d["title"])
            try:
                rating = float(d["rating"])
            except (ValueError, TypeError):
                continue
            if key not in agrupados:
                agrupados[key] = []
            agrupados[key].append(rating)
        promedios = []
        for (movie_id, title), ratings in agrupados.items():
            if ratings:
                avg_rating = sum(ratings) / len(ratings)
                promedios.append({
                    "id": movie_id,
                    "title": title,
                    "rating": avg_rating
                })
        if not promedios:
            return None
        max_rated = max(promedios, key=lambda x: x["rating"])
        min_rated = min(promedios, key=lambda x: x["rating"])

        result = prepare_data_aggregator_consult_3(min_rated, max_rated)
        return result



    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        contador = Counter()
        for fila in datos:
            nombre = fila.get("name")
            if nombre:
                contador[nombre] += 1
        top_10 = contador.most_common(10)
        resultado = [{"name": nombre, "count": cantidad} for nombre, cantidad in top_10]
        return resultado



    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        for fila in datos:
            try:
                budget = float(fila['budget'])
                revenue = float(fila['revenue'])
                if budget == 0:
                    fila['rate_revenue_budget'] = 0.0
                else:
                    fila['rate_revenue_budget'] = revenue / budget
            except (ValueError, ZeroDivisionError, KeyError):
                fila['rate_revenue_budget'] = 0.0
        sumas = defaultdict(float)
        cantidades = defaultdict(int)
        for fila in datos:
            sentimiento = fila.get('sentiment', 'UNKNOWN')
            rate = fila.get('rate_revenue_budget', 0.0)
            sumas[sentimiento] += rate
            cantidades[sentimiento] += 1
        resultado = []
        for sentimiento in sumas:
            promedio = sumas[sentimiento] / cantidades[sentimiento]
            resultado.append({
                "sentiment": sentimiento,
                "rate_revenue_budget": promedio
            })
        return resultado
    
    

    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        message_id = obtener_message_id(mensaje)
        if self.transaction.comprobar_ultima_accion(client_id, message_id, enviar_func, mensaje, canal, destino):
            logging.debug(f"Mensaje {message_id} ya procesado, ignorando")
            return
        try:
            if tipo_mensaje == EOF:
                logging.info(f"Consulta {consulta_id} de aggregator recibió EOF")
                self.eof_esperados[client_id][consulta_id] -= 1
                self.transaction.commit(EOF_ESPERADOS, [client_id, consulta_id, self.eof_esperados[client_id][consulta_id]])
                if self.eof_esperados[client_id][consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    resultado = self.ejecutar_consulta(consulta_id, client_id)
                    self.transaction.commit(ACCION, [message_id, resultado, ENVIAR])
                    enviar_func(canal, destino, resultado, mensaje, "RESULT")
                    self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])
                    self.transaction.commit(ACCION, [message_id, EOF, ENVIAR])
                    enviar_func(canal, destino, EOF, mensaje, EOF)
                    self.transaction.commit(ACCION, [message_id, "", NO_ENVIAR])
                    del self.resultados_parciales[client_id][consulta_id]
                    del self.eof_esperados[client_id][consulta_id]

                    if not self.resultados_parciales[client_id]:
                        del self.resultados_parciales[client_id]
                    if not self.eof_esperados[client_id]:
                        del self.eof_esperados[client_id]
            else:
                self.guardar_datos(consulta_id, obtener_body(mensaje), client_id)
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

    