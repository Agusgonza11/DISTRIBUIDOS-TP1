import asyncio
import logging
from common.utils import initialize_log
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

AGGREGATOR = "aggregator"

# -----------------------
# Nodo Filtro
# -----------------------
class AggregatorNode:
    def ejecutar_consulta(self, consulta_id, datos):
        # Acá iría la lógica específica para cada tipo de consulta
        lineas = datos.strip().split("\n")

        logging.info(f"Ejecutando consulta {consulta_id} con {len(lineas)} elementos")
        
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
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_2(self, datos):
        logging.info("Procesando datos para consulta 2")
        return datos

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        return datos

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        return datos

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        return datos
    


# -----------------------
# Ejecutando nodo aggregator
# -----------------------

aggregator = AggregatorNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker aggregator")
    await inicializar_comunicacion()
    await escuchar_colas(AGGREGATOR, aggregator)
    #await enviar_mock() Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

