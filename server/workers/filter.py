import asyncio
import logging
from common.utils import initialize_log
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

FILTER = "filter"

# -----------------------
# Nodo Filtro
# -----------------------
class FiltroNode:
    def ejecutar_consulta(self, consulta_id, datos):
        # Acá iría la lógica específica para cada tipo de consulta
        lineas = datos.strip().split("\n")

        logging.info(f"Ejecutando consulta {consulta_id} con {len(lineas)} elementos")
        
        match consulta_id:
            case 1:
                return self.consulta_1(datos)
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
    
    def consulta_1(self, datos):
        logging.debug("Procesando datos para consulta 1")
        return datos

    def consulta_2(self, datos):
        logging.debug("Procesando datos para consulta 2")
        return datos

    def consulta_3(self, datos):
        logging.debug("Procesando datos para consulta 3")
        return datos

    def consulta_4(self, datos):
        logging.debug("Procesando datos para consulta 4")
        return datos

    def consulta_5(self, datos):
        logging.debug("Procesando datos para consulta 5")
        return datos
    


# -----------------------
# Ejecutando nodo filtro
# -----------------------

filtro = FiltroNode()

async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker filter")
    await inicializar_comunicacion()
    await escuchar_colas(FILTER, filtro)
    #await enviar_mock() Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

