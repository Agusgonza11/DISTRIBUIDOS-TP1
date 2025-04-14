import asyncio
import logging
from common.utils import create_dataframe, initialize_log, prepare_data_filter_consult_1
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

    
    def filtro_consulta_3_4(datos):
        datos = create_dataframe(datos)
        movies_arg_post_2000 = datos[
            (datos['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (datos['release_date'].dt.year >= 2000)
        ]
        movies_arg_post_2000 = movies_arg_post_2000.astype({'id': int})
        return movies_arg_post_2000.to_csv(index=False)
    

    def consulta_1(self, datos):
        logging.info("Procesando datos para consulta 1")
        datos = prepare_data_filter_consult_1(datos)
        movies_argentina_españa_00s_df = datos[
            (datos['production_countries'].str.contains('Argentina', case=False, na=False)) & 
            (datos['production_countries'].str.contains('Spain', case=False, na=False)) & 
            (datos['release_date'].dt.year >= 2000) & 
            (datos['release_date'].dt.year < 2010)
        ]
        output_q1 = movies_argentina_españa_00s_df[["title", "genres"]]
        csv_q1 = output_q1.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q1}")
        return csv_q1

    def consulta_2(self, datos):
        logging.info("Procesando datos para consulta 2")
        datos = create_dataframe(datos)
        solo_country_df = datos[
            datos['production_countries'].apply(lambda x: len(eval(x)) == 1)
        ]
        solo_country_df = solo_country_df.copy()
        solo_country_df['country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
        csv_q2 = solo_country_df.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q2}")
        return csv_q2

    def consulta_3(self, datos):
        logging.info("Procesando datos para consulta 3")
        csv_q3 = self.filtro_consulta_3_4(datos)
        logging.info(f"lo que voy a devolver es {csv_q3}")
        return csv_q3

    def consulta_4(self, datos):
        logging.info("Procesando datos para consulta 4")
        csv_q4 = self.filtro_consulta_3_4(datos)
        logging.info(f"lo que voy a devolver es {csv_q4}")
        return csv_q4

    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        # Filtra películas válidas con budget y revenue no nulos
        datos = create_dataframe(datos)
        q5_input_df = datos.copy()
        q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
        q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]
        csv_q5 = q5_input_df.to_csv(index=False)
        logging.info(f"lo que voy a devolver es {csv_q5}")
        return csv_q5
    


# -----------------------
# Ejecutando nodo filtro
# -----------------------

filtro = FiltroNode()

async def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker filter")
    await inicializar_comunicacion()
    await escuchar_colas(FILTER, filtro)
    await enviar_mock() #Mock para probar consultas
    await asyncio.Future()

asyncio.run(main())

