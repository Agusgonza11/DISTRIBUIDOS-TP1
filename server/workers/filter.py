import logging
import os
from common.utils import cargar_cant_pnls, prepare_data_consult_1_3_4, prepare_data_consult_2, EOF, prepare_data_consult_5
from common.communication import iniciar_nodo, obtener_query

FILTER = "filter"

# -----------------------
# Nodo Filtro
# -----------------------
class FiltroNode:
    def __init__(self):
        self.nodos_pnl = cargar_cant_pnls()

    def ejecutar_consulta(self, consulta_id, datos):
        match consulta_id:
            case 1:
                return self.consulta_1(datos)
            case 2:
                return self.consulta_2(datos)
            case 3:
                logging.info("Procesando datos para consulta 3")
                return self.consulta_3_y_4(datos)
            case 4:
                logging.info("Procesando datos para consulta 4")
                return self.consulta_3_y_4(datos)
            case 5:
                return self.consulta_5(datos)
            case _:
                logging.warning(f"Consulta desconocida: {consulta_id}")
                return []

    def consulta_1(self, datos):
        logging.info("Procesando datos para consulta 1") 
        datos = prepare_data_consult_1_3_4(datos)
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
        datos = prepare_data_consult_2(datos)
        solo_country_df = datos[datos['production_countries'].apply(lambda x: len(eval(x)) == 1)]
        solo_country_df = solo_country_df.copy()
        solo_country_df.loc[:, 'country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
        return solo_country_df
    
    def consulta_3_y_4(self, datos):
        datos = prepare_data_consult_1_3_4(datos)
        movies_arg_post_2000 = datos[
            (datos['production_countries'].str.contains('Argentina', case=False, na=False)) &
            (datos['release_date'].dt.year >= 2000)
        ]
        movies_arg_post_2000 = movies_arg_post_2000.astype({'id': int})
        return movies_arg_post_2000


    def consulta_5(self, datos):
        logging.info("Procesando datos para consulta 5")
        datos = prepare_data_consult_5(datos)
        q5_input_df = datos.copy()
        q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
        q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]
        return q5_input_df


    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        try:
            if mensaje['headers'].get("type") == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                enviar_func(canal, destino, EOF, mensaje, EOF)
            else:
                resultado = self.ejecutar_consulta(consulta_id, mensaje['body'].decode('utf-8'))
                enviar_func(canal, destino, resultado, mensaje, mensaje['headers'].get("type"))
            mensaje['ack']()
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando nodo filtro
# -----------------------

if __name__ == "__main__":
    filtro = FiltroNode()
    iniciar_nodo(FILTER, filtro, os.getenv("CONSULTAS", ""))