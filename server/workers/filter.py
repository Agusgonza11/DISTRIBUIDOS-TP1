import logging
import os
from multiprocessing import Process
from common.utils import prepare_data_consult_1_3_4, prepare_data_consult_2, EOF, prepare_data_consult_5
from common.communication import  obtener_body, obtener_query, obtener_tipo_mensaje, run
from common.excepciones import ConsultaInexistente

FILTER = "filter"

# -----------------------
# Nodo Filtro
# -----------------------
class FiltroNode:
    def __init__(self):
        pass

    def eliminar(self, _):
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


    def consulta_1(self, datos_csv):
        logging.info("Procesando datos para consulta 1") 
        datos = prepare_data_consult_1_3_4(datos_csv)
        resultado = []
        for row in datos:
            fecha = row.get("release_date")
            paises = row.get("production_countries_str", "")
            if fecha and 2000 <= fecha.year < 2010:
                if "argentina" in paises and "spain" in paises:
                    resultado.append({
                        "title": row.get("title", ""),
                        "genres": row.get("genres", [])
                    })
        return resultado


    def consulta_2(self, datos):
        logging.info("Procesando datos para consulta 2")
        datos = prepare_data_consult_2(datos)
        resultado = []
        for row in datos:
            paises = row.get('production_countries', [])
            if len(paises) == 1:
                row['country'] = paises[0]
                resultado.append(row)
        return resultado
    
    
    def consulta_3_y_4(self, datos):
        datos = prepare_data_consult_1_3_4(datos)
        peliculas_filtradas = []
        for fila in datos:
            paises = fila.get("production_countries_str", "")
            fecha = fila.get("release_date")
            if fecha and fecha.year >= 2000 and "argentina" in paises:
                try:
                    fila["id"] = int(fila["id"])
                except (ValueError, TypeError):
                    continue 
                peliculas_filtradas.append(fila)
        return peliculas_filtradas



    def consulta_5(self, datos_str):
        logging.info("Procesando datos para consulta 5")
        datos_filtrados = prepare_data_consult_5(datos_str)
        return datos_filtrados


    def procesar_mensajes(self, canal, destino, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        try:
            if tipo_mensaje == EOF:
                logging.info(f"Consulta {consulta_id} recibió EOF")
                enviar_func(canal, destino, EOF, mensaje, EOF)
            else:
                resultado = self.ejecutar_consulta(consulta_id, obtener_body(mensaje))
                enviar_func(canal, destino, resultado, mensaje, tipo_mensaje)
            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")



# -----------------------
# Ejecutando nodo filtro
# -----------------------

if __name__ == "__main__":
    run(FILTER, FiltroNode)
