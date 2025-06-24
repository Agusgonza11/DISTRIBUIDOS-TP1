from multiprocessing import Process
import os
import sys
import logging
from common.utils import EOF,  cargar_datos_broker, cargar_eofs, obtener_nombre_contenedor
from common.communication import  obtener_body, obtener_client_id, obtener_query, obtener_tipo_mensaje, run
from common.exceptions import ConsultaInexistente, ErrorCargaDelEstado

from common.transaction import ACCION, Transaction


BROKER = "broker"
TMP_DIR = f"/tmp/{obtener_nombre_contenedor(BROKER)}_tmp"

CONSULTA_3 = 0
CONSULTA_4 = 1
CONSULTA_5 = 2

ULTIMO_NODO = "ultimo_nodo_consulta"
EOF_ESPERA = "eof_esperar"

# -----------------------
# Broker
# -----------------------
class Broker:
    def __init__(self):
        self.nodos_enviar = {}
        self.eof_esperar = {}
        self.ultimo_nodo_consulta = {}
        self.clients = []
        self.transaction = Transaction(TMP_DIR)
        self.transaction.cargar_estado(self)

    
    def reconstruir_estado(self, clave, contenido):
        client_id, request_id, valor = contenido.split("|", 2)
        if client_id not in self.nodos_enviar:
            self.create_client(client_id)
        request_id = int(request_id)
        match clave:
            case "ultimo_nodo_consulta":
                if request_id not in self.ultimo_nodo_consulta[client_id]:
                    self.ultimo_nodo_consulta[client_id][request_id] = 0
                self.ultimo_nodo_consulta[client_id][request_id] = int(valor)
            case "eof_esperar":
                if request_id not in self.eof_esperar[client_id]:
                    self.eof_esperar[client_id][request_id] = 0
                self.eof_esperar[client_id][request_id] = int(valor)
            case _:
                raise ErrorCargaDelEstado(f"Error en la carga del estado")

    def create_client(self, client):
        self.nodos_enviar[client] = cargar_datos_broker()
        self.eof_esperar[client] = cargar_eofs()
        self.ultimo_nodo_consulta[client] = [self.nodos_enviar[client][3][0], self.nodos_enviar[client][4][0], 1]
        self.clients.append(client)


    def eliminar(self, es_global):
        self.nodos_enviar.clear()
        self.eof_esperar.clear()
        self.ultimo_nodo_consulta.clear()
        self.clients.clear()
        if es_global:
            try:
                self.transaction.borrar_carpeta()
                logging.info(f"Volumen limpiado por shutdown global")
            except Exception as e:
                logging.error(f"Error limpiando volumen en shutdown global: {e}")


    def siguiente_nodo(self, request_id, client_id):
        if request_id == 5:
            self.ultimo_nodo_consulta[client_id][CONSULTA_5] += 1
            if self.ultimo_nodo_consulta[client_id][CONSULTA_5] > self.nodos_enviar[client_id][5]:
                self.ultimo_nodo_consulta[client_id][CONSULTA_5] = 1      
            self.transaction.commit(ULTIMO_NODO, [client_id, CONSULTA_5, self.ultimo_nodo_consulta[client_id][CONSULTA_5]])            
        elif request_id == 3:
            lista_nodos = self.nodos_enviar[client_id][3]
            idx_actual = lista_nodos.index(self.ultimo_nodo_consulta[client_id][CONSULTA_3])
            idx_siguiente = (idx_actual + 1) % len(lista_nodos)
            self.ultimo_nodo_consulta[client_id][CONSULTA_3] = lista_nodos[idx_siguiente]     
            self.transaction.commit(ULTIMO_NODO, [client_id, CONSULTA_3, lista_nodos[idx_siguiente]])       
        else:
            lista_nodos = self.nodos_enviar[client_id][4]
            idx_actual = lista_nodos.index(self.ultimo_nodo_consulta[client_id][CONSULTA_4])
            idx_siguiente = (idx_actual + 1) % len(lista_nodos)
            self.ultimo_nodo_consulta[client_id][CONSULTA_4] = lista_nodos[idx_siguiente]  
            self.transaction.commit(ULTIMO_NODO, [client_id, CONSULTA_4, lista_nodos[idx_siguiente]])



    def distribuir_informacion(self, client_id, request_id, mensaje, canal, enviar_func, tipo=None):
        if request_id == 5:
            for pnl_id in range(1, self.nodos_enviar[client_id][request_id] + 1):
                destino = f'pnl_request_5_{pnl_id}'
                enviar_func(canal, destino, obtener_body(mensaje), mensaje, tipo)
        else:
            for joiner_id in self.nodos_enviar[client_id][request_id]:
                destino = f'joiner_request_{request_id}_{joiner_id}'
                body = EOF if tipo != "MOVIES" else obtener_body(mensaje)
                enviar_func(canal, destino, body, mensaje, tipo)


    def distribuir_informacion_round_robin(self, client_id, request_id, mensaje, canal, enviar_func, tipo=None):
        if request_id == 5:
            destino = f'pnl_request_5_{self.ultimo_nodo_consulta[client_id][CONSULTA_5]}'
        elif request_id == 3:
            destino = f'joiner_request_3_{self.ultimo_nodo_consulta[client_id][CONSULTA_3]}'
        elif request_id == 4:
            destino = f'joiner_request_4_{self.ultimo_nodo_consulta[client_id][CONSULTA_4]}'
        self.siguiente_nodo(request_id, client_id)
        enviar_func(canal, destino, obtener_body(mensaje), mensaje, tipo)


    def procesar_mensajes(self, canal, _, mensaje, enviar_func):
        request_id = obtener_query(mensaje)
        tipo_mensaje = obtener_tipo_mensaje(mensaje)
        client_id = obtener_client_id(mensaje)
        if client_id not in self.clients:
            self.create_client(client_id)
        try:
            if request_id == 5:
                if tipo_mensaje == EOF:
                    self.eof_esperar[client_id][request_id] -= 1
                    self.transaction.commit(EOF_ESPERA, [client_id, request_id, self.eof_esperar[client_id][request_id]])
                    if self.eof_esperar[client_id][request_id] == 0:
                        self.distribuir_informacion(client_id, request_id, mensaje, canal, enviar_func, EOF)
                else:
                    self.distribuir_informacion_round_robin(client_id, request_id, mensaje, canal, enviar_func)
            else:
                if tipo_mensaje in {"EOF_CREDITS", "EOF_RATINGS"}:
                    logging.info(f"Recibi EOF: {tipo_mensaje}")
                if tipo_mensaje in {"MOVIES", "EOF_CREDITS", "EOF_RATINGS"}:
                    self.distribuir_informacion(client_id, request_id, mensaje, canal, enviar_func, tipo_mensaje)

                elif tipo_mensaje in {"CREDITS", "RATINGS"}:
                    self.distribuir_informacion_round_robin(client_id, request_id, mensaje, canal, enviar_func, tipo_mensaje)

                elif tipo_mensaje == EOF:
                    self.eof_esperar[client_id][request_id] -= 1
                    self.transaction.commit(EOF_ESPERA, [client_id, request_id, self.eof_esperar[client_id][request_id]])
                    if self.eof_esperar[client_id][request_id] == 0:
                        self.distribuir_informacion(client_id, request_id, mensaje, canal, enviar_func, EOF)

                else:
                    logging.error(f"Tipo de mensaje inesperado en consulta {request_id}: {tipo_mensaje}")
            mensaje['ack']()
        except ConsultaInexistente as e:
            logging.warning(f"Consulta inexistente: {e}")    
        except Exception as e:
            logging.error(f"Error procesando mensaje en consulta {request_id}: {e}")



# -----------------------
# Ejecutando broker
# -----------------------

if __name__ == "__main__":
    run(BROKER, Broker)
