import sys
import logging
from common.utils import EOF, cargar_datos_broker, cargar_eofs
from common.communication import iniciar_nodo, obtener_body, obtener_query, obtener_tipo_mensaje

BROKER = "broker"

CONSULTA_3 = 0
CONSULTA_4 = 1
CONSULTA_5 = 2

# -----------------------
# Broker
# -----------------------
class Broker:
    def __init__(self):
        self.nodos_enviar = cargar_datos_broker()
        self.eof_esperar = cargar_eofs()
        self.ultimo_nodo_consulta = [self.nodos_enviar[3][0], self.nodos_enviar[4][0], 1]


    def eliminar(self):
        self.nodos_enviar = None
        self.eof_esperar = None

    def siguiente_nodo(self, consulta_id):
        if consulta_id == 5:
            self.ultimo_nodo_consulta[CONSULTA_5] += 1
            if self.ultimo_nodo_consulta[CONSULTA_5] > self.nodos_enviar[5]:
                self.ultimo_nodo_consulta[CONSULTA_5] = 1           
        elif consulta_id == 3:
            lista_nodos = self.nodos_enviar[3]
            idx_actual = lista_nodos.index(self.ultimo_nodo_consulta[CONSULTA_3])
            idx_siguiente = (idx_actual + 1) % len(lista_nodos)
            self.ultimo_nodo_consulta[CONSULTA_3] = lista_nodos[idx_siguiente]            
        else:
            lista_nodos = self.nodos_enviar[4]
            idx_actual = lista_nodos.index(self.ultimo_nodo_consulta[CONSULTA_4])
            idx_siguiente = (idx_actual + 1) % len(lista_nodos)
            self.ultimo_nodo_consulta[CONSULTA_4] = lista_nodos[idx_siguiente]        


    def distribuir_informacion(self, consulta_id, mensaje, canal, enviar_func, tipo=None):
        if consulta_id == 5:
            for pnl_id in range(1, self.nodos_enviar[consulta_id] + 1):
                destino = f'pnl_consult_5_{pnl_id}'
                enviar_func(canal, destino, obtener_body(mensaje), mensaje, tipo)
        else:
            for joiner_id in self.nodos_enviar[consulta_id]:
                destino = f'joiner_consult_{consulta_id}_{joiner_id}'
                body = EOF if tipo != "MOVIES" else obtener_body(mensaje)
                enviar_func(canal, destino, body, mensaje, tipo)


    def distribuir_informacion_random(self, consulta_id, mensaje, canal, enviar_func, tipo=None):
        if consulta_id == 5:
            destino = f'pnl_consult_5_{self.ultimo_nodo_consulta[CONSULTA_5]}'
        elif consulta_id == 3:
            destino = f'joiner_consult_3_{self.ultimo_nodo_consulta[CONSULTA_3]}'
        elif consulta_id == 4:
            destino = f'joiner_consult_4_{self.ultimo_nodo_consulta[CONSULTA_4]}'
        self.siguiente_nodo(consulta_id)
        enviar_func(canal, destino, obtener_body(mensaje), mensaje, tipo)


    def procesar_mensajes(self, canal, _, mensaje, enviar_func):
        consulta_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)

        try:
            if consulta_id == 5:
                if tipo_mensaje == EOF:
                    self.eof_esperar[consulta_id] -= 1
                    if self.eof_esperar[consulta_id] == 0:
                        self.distribuir_informacion(consulta_id, mensaje, canal, enviar_func, EOF)
                else:
                    self.distribuir_informacion_random(consulta_id, mensaje, canal, enviar_func)
            else:
                if tipo_mensaje in {"MOVIES", "EOF_CREDITS", "EOF_RATINGS"}:
                    self.distribuir_informacion(consulta_id, mensaje, canal, enviar_func, tipo_mensaje)

                elif tipo_mensaje in {"CREDITS", "RATINGS"}:
                    self.distribuir_informacion_random(consulta_id, mensaje, canal, enviar_func, tipo_mensaje)

                elif tipo_mensaje == EOF:
                    self.eof_esperar[consulta_id] -= 1
                    if self.eof_esperar[consulta_id] == 0:
                        self.distribuir_informacion(consulta_id, mensaje, canal, enviar_func, EOF)

                else:
                    logging.error(f"Tipo de mensaje inesperado en consulta {consulta_id}: {tipo_mensaje}")

            mensaje['ack']()

        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {consulta_id}: {e}")


# -----------------------
# Ejecutando broker
# -----------------------

if __name__ == "__main__":
    broker = Broker()
    iniciar_nodo(BROKER, broker)

