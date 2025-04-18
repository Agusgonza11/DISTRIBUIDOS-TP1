import asyncio
import logging
import os
from common.utils import cargar_eofs, create_dataframe, initialize_log, prepare_data_aggregator_consult_3
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

JOINER = "joiner"

# -----------------------
# Nodo Filtro
# -----------------------
class AggregatorNode:
    def __init__(self):
        self.resultados_parciales = {}
        self.eof_esperados = cargar_eofs()

    def guardar_datos(self, consulta_id, datos):
        #Tomas aca guardar en memoria y base de datos
        if consulta_id not in self.resultados_parciales:
            self.resultados_parciales[consulta_id] = []
        self.resultados_parciales[consulta_id].append(datos)

    def ejecutar_consulta(self, consulta_id):
        datos = "\n".join(self.resultados_parciales.get(consulta_id, []))
        lineas = datos.strip().split("\n")
        logging.info(f"Ejecutando consulta {consulta_id} con {len(lineas)} elementos")
        
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
        datos = create_dataframe(datos)
        #Consulta 3 Tomas
        csv_q3 = datos.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q3}")
        return csv_q3

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        datos = create_dataframe(datos)
        #Consulta 4 Tomas
        csv_q4 = datos.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q4}")
        return csv_q4

    
    async def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
        logging.info(f"la cantidad de eof es {self.eof_esperados}")
        if mensaje.headers.get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.eof_esperados[consulta_id] -= 1
            if self.eof_esperados[consulta_id] == 0:
                logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                resultado = self.ejecutar_consulta(consulta_id)
                await enviar_func(destino, "EOF", headers={"type": "EOF"})
                await enviar_func(destino, "EOF")
                return
        self.guardar_datos(consulta_id, mensaje.body.decode('utf-8'))


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

aggregator = AggregatorNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker joiner")
    consultas_str = os.getenv("CONSULTAS", "")
    consultas = list(map(int, consultas_str.split(","))) if consultas_str else []

    await inicializar_comunicacion()
    await escuchar_colas(JOINER, aggregator, consultas)
    #await enviar_mock() # Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

