import psycopg2
import logging
from time import sleep

class DBClient:
    def __init__(self):
        self.conn = self._connect_with_retries()
        self.cur = self.conn.cursor()

    def _connect_with_retries(self, retries=5, delay=5):
        for i in range(retries):
            try:
                return psycopg2.connect(
                    host="postgres",
                    database="datos",
                    user="user",
                    password="password"
                )
            except psycopg2.OperationalError as e:
                logging.warning(f"Intento {i + 1}: No se pudo conectar a PostgreSQL: {e}")
                sleep(delay)
        raise Exception("No se pudo conectar a la base de datos después de varios intentos")

    def insertar_csv(self, nombre_tabla, datos_csv):
        self.cur.copy_from(datos_csv, nombre_tabla, sep=",")
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

