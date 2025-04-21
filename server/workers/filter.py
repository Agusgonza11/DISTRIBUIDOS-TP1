import asyncio
import logging
from common.utils import cargar_eof_a_enviar, create_dataframe, initialize_log, prepare_data_filter
from workers.test import enviar_mock
from workers.communication import inicializar_comunicacion, escuchar_colas
import os

FILTER = "filter"

# -----------------------
# Nodo Filtro
# -----------------------
class FiltroNode:
    def __init__(self):
        self.eof_a_enviar = cargar_eof_a_enviar()
        self.shutdown_event = asyncio.Event()


    def ejecutar_consulta(self, consulta_id, datos):
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

    
    def filtro_consulta_3_4(self, datos):
        datos = prepare_data_filter(datos)
        movies_arg_post_2000 = datos[
            (datos['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (datos['release_date'].dt.year >= 2000)
        ]
        movies_arg_post_2000 = movies_arg_post_2000.astype({'id': int})
        return movies_arg_post_2000
    

    def consulta_1(self, datos):
        logging.info("Procesando datos para consulta 1")
        datos = prepare_data_filter(datos)
        movies_argentina_españa_00s_df = datos[
            (datos['production_countries'].str.contains('Argentina', case=False, na=False)) & 
            (datos['production_countries'].str.contains('Spain', case=False, na=False)) & 
            (datos['release_date'].dt.year >= 2000) & 
            (datos['release_date'].dt.year < 2010)
        ]
        output_q1 = movies_argentina_españa_00s_df[["title", "genres"]]
        return output_q1

    def consulta_2(self, datos):
        logging.info("Procesando datos para consulta 2")
        datos = create_dataframe(datos)
        solo_country_df = datos[
            datos['production_countries'].apply(lambda x: len(eval(x)) == 1)
        ]
        solo_country_df = solo_country_df.copy()
        solo_country_df['country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
        return solo_country_df

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        return self.filtro_consulta_3_4(datos)

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        return self.filtro_consulta_3_4(datos)

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos = create_dataframe(datos)
        q5_input_df = datos.copy()
        q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
        q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]
        return q5_input_df
    
    async def procesar_mensajes(self, destino, consulta_id, mensaje, enviar_func):
        if mensaje.headers.get("type") == "EOF":
            logging.info(f"Consulta {consulta_id} recibió EOF")
            eof_a_enviar = self.eof_a_enviar.get(consulta_id, 1) if consulta_id in [3, 4, 5] else 1
            for _ in range(eof_a_enviar):
                await enviar_func(destino, "EOF", headers={"type": "EOF","Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")})
            self.shutdown_event.set()
            return
        resultado = self.ejecutar_consulta(consulta_id, mensaje.body.decode('utf-8'))
        headers = {"type": "MOVIES", "Query": consulta_id, "ClientID": mensaje.headers.get("ClientID")} if consulta_id in [3, 4] else None
        await enviar_func(destino, resultado, headers=headers)

    


# -----------------------
# Ejecutando nodo filtro
# -----------------------

filtro = FiltroNode()

async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker filter")
    consultas_str = os.getenv("CONSULTAS", "")
    consultas = list(map(int, consultas_str.split(","))) if consultas_str else []
    await inicializar_comunicacion()
    await escuchar_colas(FILTER, filtro, consultas)
    await enviar_mock() #Mock para probar consultas
    await filtro.shutdown_event.wait()
    logging.info("Shutdown del nodo filtro")

asyncio.run(main())

