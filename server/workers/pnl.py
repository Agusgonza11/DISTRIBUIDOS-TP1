import asyncio
import logging
from common.utils import initialize_log
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas_para_pnl


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

async def procesar_mensajes(consulta_id, contenido, enviar_func):
    if contenido.strip() == b"EOF":
        logging.info(f"Consulta {consulta_id} pnl recibió EOF")
        return
    resultado = pnl.ejecutar_consulta(consulta_id, contenido)
    destino = f"gateway_output_{consulta_id}"
    await enviar_func(destino, resultado)


async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker pnl")
    await inicializar_comunicacion()
    await escuchar_colas_para_pnl(procesar_mensajes)
    #await enviar_mock() Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

