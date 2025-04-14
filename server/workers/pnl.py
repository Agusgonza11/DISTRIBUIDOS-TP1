import asyncio
import logging
from common.utils import initialize_log
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas

PNL = "pnl"

# -----------------------
# Nodo Filtro
# -----------------------
class PnlNode:
    def ejecutar_consulta(self, consulta_id, datos):
        # Acá iría la lógica específica para cada tipo de consulta
        lineas = datos.strip().split("\n")

        logging.info(f"Ejecutando consulta {consulta_id} con {len(lineas)} elementos")
        
        match consulta_id:
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []
    

    def consulta_5(self, datos):
        logging.debug("Procesando datos para consulta 5")
        return datos
    


# -----------------------
# Ejecutando nodo pnl
# -----------------------

pnl = PnlNode()


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker pnl")
    await inicializar_comunicacion()
    await escuchar_colas(PNL, pnl)
    #await enviar_mock() Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

