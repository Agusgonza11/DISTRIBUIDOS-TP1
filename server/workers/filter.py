import asyncio
import aio_pika
import json
import logging
from common.utils import initialize_log, esperar_conexion

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
                return self.consulta_1(lineas)
            case 2:
                return self.consulta_2(lineas)
            case 3:
                return self.consulta_3(lineas)
            case 4:
                return self.consulta_4(lineas)
            case 5:
                return self.consulta_5(lineas)
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

async def procesar_mensaje(mensaje: aio_pika.IncomingMessage, consulta_id: int):
    async with mensaje.process():
        try:
            batch = mensaje.body.decode() #Me llega el batch
            if batch.strip() == "EOF":
                logging.info(f"Consulta {consulta_id} recibió EOF")
                return
            
            resultado = filtro.ejecutar_consulta(consulta_id, batch)

            # Publicamos el resultado en una cola única por consulta (opcional)
            if consulta_id == 1:
                await mensaje.channel.default_exchange.publish(
                    aio_pika.Message(body="\n".join(resultado).encode()),
                    routing_key=f"gateway_output_{consulta_id}"
                )
            else:
                await mensaje.channel.default_exchange.publish(
                    aio_pika.Message(body="\n".join(resultado).encode()),
                    routing_key=f"aggregator_consult_{consulta_id}"
                )
        except Exception as e:
            print(f"Error procesando mensaje en consulta {consulta_id}: {e}")

async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker filter")
    conexion = await esperar_conexion()
    canal = await conexion.channel()
    await canal.set_qos(prefetch_count=1)

    # Crear y consumir desde 5 colas diferentes
    for consulta_id in range(1, 6):
        queue_name = f"filter_consult_{consulta_id}"
        await canal.declare_queue(queue_name, durable=True)
        if consulta_id == 1:
            await canal.declare_queue(f"gateway_output_{consulta_id}", durable=True)
        else:
            await canal.declare_queue(f"aggregator_consult_{consulta_id}", durable=True)

        # Creamos una función wrapper con el número de consulta ya definido
        async def callback_wrapper(msg, consulta_id=consulta_id):
            await procesar_mensaje(msg, consulta_id)

        await canal.set_qos(prefetch_count=1)
        queue = await canal.get_queue(queue_name)
        await queue.consume(callback_wrapper)
        logging.info(f"Filtro escuchando en {queue_name}")

    await asyncio.Future()

asyncio.run(main())

