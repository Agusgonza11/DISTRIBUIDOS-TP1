import logging
import os
from multiprocessing import Process
from common.utils import prepare_data_consult_1_3_4, prepare_data_consult_2, EOF, prepare_data_consult_5
from common.communication import iniciar_nodo, obtener_body, obtener_query, obtener_tipo_mensaje
from common.excepciones import ConsultaInexistente
from common.health import HealthMonitor

FILTER = "filter"

# -----------------------
# Nodo Filtro
# -----------------------
class FiltroNode:
    def eliminar(self):
        pass

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
                raise ConsultaInexistente(f"Consulta {consulta_id} no encontrada")

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
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        mensaje['ack']()
        try:
            if tipo_mensaje == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                enviar_func(canal, destino, EOF, mensaje, EOF)
            else:
                resultado = self.ejecutar_consulta(consulta_id, obtener_body(mensaje))
                enviar_func(canal, destino, resultado, mensaje, tipo_mensaje)
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando nodo filtro
# -----------------------

if __name__ == "__main__":
    proceso_nodo = Process(target=iniciar_nodo, args=(FILTER, FiltroNode(), os.getenv("CONSULTAS", "")))
    monitor = HealthMonitor()
    proceso_monitor = Process(target=monitor.run)

    # Iniciar ambos procesos
    proceso_nodo.start()
    proceso_monitor.start()

    # Esperar a que terminen (opcional)
    proceso_nodo.join()
    proceso_monitor.join()
