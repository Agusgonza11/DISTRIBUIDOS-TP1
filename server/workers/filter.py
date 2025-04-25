import logging
import threading
import os
from common.utils import cargar_eof_a_enviar, create_dataframe, prepare_data_filter, EOF
from common.communication import determinar_salida, iniciar_nodo, obtener_query

FILTER = "filter"

# -----------------------
# Nodo Filtro
# -----------------------
class FiltroNode:
    def __init__(self):
        self.eof_a_enviar = cargar_eof_a_enviar()
        self.shutdown_event = threading.Event()

    def ejecutar_consulta(self, consulta_id, datos):
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


    def procesar_mensajes(self, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                eof_a_enviar = self.eof_a_enviar.get(consulta_id, 1) if consulta_id in [3, 4, 5] else 1
                for _ in range(eof_a_enviar):
                    enviar_func(FILTER, consulta_id, EOF, mensaje, EOF)
                self.shutdown_event.set()
            else:
                resultado = self.ejecutar_consulta(consulta_id, mensaje['body'].decode('utf-8'))
                tipo = "MOVIES" if consulta_id in [3, 4] else None
                enviar_func(FILTER, consulta_id, resultado, mensaje, tipo)

            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando nodo filtro
# -----------------------

if __name__ == "__main__":
    filtro = FiltroNode()
    iniciar_nodo(FILTER, filtro, os.getenv("CONSULTAS", ""))
