import threading
import logging
import os
from common.utils import EOF, cargar_eofs, concat_data, create_dataframe, dictionary_to_list
from common.communication import iniciar_nodo, obtener_query
import pandas as pd # type: ignore


JOINER = "joiner"

# -----------------------
# Nodo Joiner
# -----------------------
class JoinerNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = threading.Event()
        self.eof_esperados = cargar_eofs()
        self.termino_credits = False
        self.termino_movies = False
        self.termino_ratings = False
        self.lineas_csv = {"ratings": 0, "credits": 0}
        self.umbral_envio_3 = 10000
        self.umbral_envio_4 = 10000
        self.datos_4 = []
        self.datos_3 = []



    def puede_enviar(self, consulta_id):
        logging.info(f"puede enviar {self.lineas_csv}")
        puede_enviar = False
        if consulta_id == 3 and (self.lineas_csv["ratings"] >= self.umbral_envio_3 or self.termino_ratings):
            self.lineas_csv["ratings"] = 0
            puede_enviar = True
        if consulta_id == 4 and (self.lineas_csv["credits"] >= self.umbral_envio_4 or self.termino_credits):
            self.lineas_csv["credits"] = 0
            puede_enviar = True        
        return puede_enviar


    def guardar_datos(self, consulta_id, datos):
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        self.resultados_parciales[consulta_id].append(create_dataframe(datos))

    def guardar_datos_temporal(self, consulta, datos):
        if consulta == 3:
            self.datos_3.append(create_dataframe(datos))
            self.lineas_csv["ratings"] += len(datos)
        else:
            self.datos_4.append(create_dataframe(datos))
            self.lineas_csv["credits"] += len(datos)


    def ejecutar_consulta(self, consulta_id):
        datos = self.resultados_parciales.get(consulta_id, [])
        if not datos:
            return False

        datos = concat_data(datos)
        
        match consulta_id:
            case 3:
                return self.consulta_3(datos)
            case 4:
                return self.consulta_4(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        ratings = concat_data(self.datos_3)
        if ratings.empty:
            return False
        ratings_df = pd.DataFrame(ratings, columns=["id", "rating"])
        ratings_df.rename(columns={"movieId": "id"}, inplace=True)

        return datos.merge(ratings_df, on="id")

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        credits = concat_data(self.datos_4)
        if credits.empty:
            return False
        credits.columns = ['id', 'cast']
        credits['cast'] = credits['cast'].apply(dictionary_to_list)
        credits.rename(columns={"movieId": "id"}, inplace=True)
        return datos.merge(credits, on="id")

    
    def consulta_completa(self, consulta_id):
        match consulta_id:
            case 3:
                return self.termino_ratings and self.termino_movies
            case 4:
                return self.termino_credits and self.termino_movies
            case _:
                return self.termino_movies

    def termino_nodo(self):
        return self.termino_credits and self.termino_movies and self.termino_ratings
    
    def procesar_mensajes(self, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            # Manejo de EOF
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                self.eof_esperados[consulta_id] -= 1
                if self.eof_esperados[consulta_id] == 0:
                    logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                    self.termino_movies = True
                    mensaje['ack']()  # ACK después de recibir todos los EOF

            # Manejo de EOF_RATINGS
            if mensaje['headers'].get("type") == "EOF_RATINGS":
                logging.info(f"Recibí todos los ratings")
                self.termino_ratings = True
                mensaje['ack']()  # ACK después de recibir todos los ratings

            # Manejo de EOF_CREDITS
            if mensaje['headers'].get("type") == "EOF_CREDITS":
                logging.info(f"Recibí todos los credits")
                self.termino_credits = True
                mensaje['ack']()  # ACK después de recibir todos los credits

            # Guardado de datos de tipo RATINGS
            if mensaje['headers'].get("type") == "RATINGS":
                self.guardar_datos_temporal(consulta_id, mensaje['body'].decode('utf-8'))
                mensaje['ack']()  # ACK después de guardar los ratings

            # Guardado de datos temporales para CREDITS
            if mensaje['headers'].get("type") == "CREDITS":
                self.guardar_datos_temporal(consulta_id, mensaje['body'].decode('utf-8'))
                mensaje['ack']()  # ACK después de guardar los credits

            # Guardado de datos de tipo MOVIES
            if mensaje['headers'].get("type") == "MOVIES":
                self.guardar_datos(consulta_id, mensaje['body'].decode('utf-8'))
                mensaje['ack']()  # ACK después de guardar las películas

            # Envío de resultado si se han recibido todos los datos necesarios
            if self.termino_movies and self.puede_enviar(consulta_id):
                resultado = self.ejecutar_consulta(consulta_id)
                enviar_func(JOINER, consulta_id, resultado, mensaje, "")
            
            # Envío de EOF cuando la consulta esté completa
            if self.consulta_completa(consulta_id):
                enviar_func(JOINER, consulta_id, EOF, mensaje, EOF)

            # Verificación de si el nodo debe apagarse
            if self.termino_nodo():
                self.shutdown_event.set()
                mensaje['ack']()  # ACK final antes de apagar el nodo

        except Exception as e:
            logging.error(f"Error procesando mensaje para consulta {consulta_id}: {e}")
            # No se hace ack en caso de error, lo que permitirá reintentar el mensaje


# -----------------------
# Ejecutando nodo joiner
# -----------------------

if __name__ == "__main__":
    joiner = JoinerNode()
    iniciar_nodo(JOINER, joiner, os.getenv("CONSULTAS", ""))

