import asyncio
import logging
import os
from common.utils import cargar_eofs, create_dataframe, initialize_log
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas
from transformers import pipeline # type: ignore
import time

PNL = "pnl"

# -----------------------
# Nodo Filtro
# -----------------------
class PnlNode:
    def __init__(self):
        self.eof_esperados = cargar_eofs()


    def ejecutar_consulta(self, consulta_id, datos):
        lineas = datos.strip().split("\n")

        logging.info(f"Ejecutando consulta {consulta_id} con {len(lineas)} elementos")
        
        match consulta_id:
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos = create_dataframe(datos)
        sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        datos['sentiment'] = datos['overview'].fillna('').apply(lambda x: sentiment_analyzer(x)[0]['label'])
        csv_q5 = datos.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q5}")
        return csv_q5
    
    async def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
        if mensaje.headers.get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            self.eof_esperados[consulta_id] -= 1
            if self.eof_esperados[consulta_id] == 0:
                logging.info(f"Consulta {consulta_id} recibió TODOS los EOF que esperaba")
                await enviar_func(destino, "EOF")
                return
        resultado = self.ejecutar_consulta(consulta_id, mensaje.body.decode('utf-8'))
        await enviar_func(destino, resultado)

    


# -----------------------
# Ejecutando nodo pnl
# -----------------------

pnl = PnlNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker pnl")
    consultas_str = os.getenv("CONSULTAS", "")
    consultas = list(map(int, consultas_str.split(","))) if consultas_str else []
    await inicializar_comunicacion()
    await escuchar_colas(PNL, pnl, consultas)
    #await enviar_mock() # Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

