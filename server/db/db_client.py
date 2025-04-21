import psycopg2
import logging
from io import StringIO

class DBClient:

    MAX_REGISTROS = 1000

    def __init__(self):
        self.conn = self._conectar()
        self.cur = self.conn.cursor()
        self._create_tables()

    def _conectar(self):
            try:
                return psycopg2.connect(
                    host="postgres",
                    database="datos",
                    user="user",
                    password="password",
                )
            except Exception as e:
                logging.error(f"No se pudo conectar a PostgreSQL: {e}")

    def guardar_csv(self, nombre_tabla, datos_csv):
        buffer = StringIO(datos_csv.strip())
        self.cur.copy_from(buffer, nombre_tabla, sep=",")
        self.conn.commit()

    def obtener_ratings(self):
        self.cur.execute("DELETE FROM ratings WHERE movie_id IN (SELECT movie_id FROM ratings LIMIT %s) RETURNING *;", (self.MAX_REGISTROS,))
        resultados = self.cur.fetchall()
        self.conn.commit()
        return resultados
    
    def obtener_credits(self):
        self.cur.execute("DELETE FROM credits WHERE movie_id IN (SELECT movie_id FROM credits LIMIT %s) RETURNING *;", (self.MAX_REGISTROS,))
        resultados = self.cur.fetchall()
        self.conn.commit()
        return resultados

    def _create_tables(self):
        create_ratings_table = """
            CREATE TABLE ratings (
                movie_id INTEGER PRIMARY KEY,
                rating FLOAT
            );
        """
        create_credits_table = """
            CREATE TABLE credits (
                movie_id INTEGER PRIMARY KEY,
                cast_list TEXT[]
            );
        """
        self.cur.execute(create_ratings_table)
        self.cur.execute(create_credits_table)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

