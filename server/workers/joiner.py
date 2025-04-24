import asyncio
import logging
import os
from common.utils import cargar_eofs, concat_data, create_dataframe, initialize_log, prepare_data_aggregator_consult_3
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

JOINER = "joiner"

# -----------------------
# Nodo Filtro
# -----------------------
class JoinerNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.shutdown_event = asyncio.Event()
        self.eof_esperados = cargar_eofs()
        self.termino_credits = False
        self.termino_movies = False
        self.termino_ratings = False

    def guardar_csv(self, csv, datos):
        if csv == "RATINGS":
            pass
        if csv == "CREDITS":
            pass
        #Aca deberia guardar en base de datos
        # Datos es un DataFrame
        pass

    def puede_enviar(self):
        #Aca deberia ser una funcion que decida cuando puede enviar al nodo siguiente el resultado
        #Quiza esperar una cierta cantidad de info hay que ver
        False

    def guardar_datos(self, consulta_id, datos):
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        self.resultados_parciales[consulta_id].append(create_dataframe(datos))

    def ejecutar_consulta(self, consulta_id):
        datos = self.resultados_parciales.get(consulta_id, [])
        if not datos:
            return False

        datos = concat_data(datos)
        logging.info(f"Ejecutando consulta {consulta_id} con {len(datos)} elementos")      
        
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
        #Consulta 3 Tomas
        return datos

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        #Consulta 4 Tomas
        return datos

    
    async def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
        if mensaje.headers.get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.eof_esperados[consulta_id] -= 1
            if self.eof_esperados[consulta_id] == 0:
                logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                self.termino_movies = True
                return
        if mensaje.headers.get("EOF_RATINGS") == True:
            self.termino_ratings = True
        if mensaje.headers.get("EOF_CREDITS") == True:
            self.termino_credits = True
        if mensaje.headers.get("type") == "RATINGS":
            self.guardar_csv("ratings", mensaje.body.decode('utf-8'))
        if mensaje.headers.get("type") == "CREDITS":
            self.guardar_csv("credits", mensaje.body.decode('utf-8'))
        if mensaje.headers.get("type") == "MOVIES":
            self.guardar_datos(consulta_id, mensaje.body.decode('utf-8'))
        if self.termino_movies or self.puede_enviar():
            resultado = self.ejecutar_consulta(consulta_id)
            await enviar_func(destino, resultado, headers={"Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")})
        if self.termino_credits and self.termino_ratings and self.termino_movies:
            await enviar_func(destino, "EOF", headers={"type": "EOF", "Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")})
            self.shutdown_event.set()


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

joiner = JoinerNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker joiner")
    consultas_str = os.getenv("CONSULTAS", "")
    consultas = list(map(int, consultas_str.split(","))) if consultas_str else []

    await inicializar_comunicacion()
    await escuchar_colas(JOINER, joiner, consultas)
    #await enviar_mock() # Mock para probar consultas
    await joiner.shutdown_event.wait()
    logging.info("Shutdown del nodo joiner")

asyncio.run(main())

