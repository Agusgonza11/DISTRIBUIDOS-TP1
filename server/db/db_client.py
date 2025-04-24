import psycopg2
import logging
from io import StringIO
import ast
import csv as csv_reader

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
        logging.info(f"CAST {datos_csv}")
        if nombre_tabla == "credits":
            self.guardar_credits(datos_csv)
        else:
            buffer = StringIO(datos_csv.strip())
            self.cur.copy_from(buffer, nombre_tabla, sep=",")
            self.conn.commit()

    def guardar_credits(self, datos_csv):
        buffer = StringIO(datos_csv.strip())
        reader = csv_reader.reader(buffer)
        for movie_id, cast_str, in reader:
            try:
                actores = ast.literal_eval(cast_str)
                for actor in actores:
                    self.cur.execute(
                        "INSERT INTO credits (movie_id, actor) VALUES (%s, %s);",
                        (int(movie_id), actor['name'])
                    )
            except Exception as e:
                logging.warning(f"Error procesando fila con movie_id={movie_id}: {e}")
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
                movie_id INTEGER,
                rating FLOAT
            );
        """
        create_credits_table = """
            CREATE TABLE credits (
                movie_id INTEGER,
                actor TEXT
            );
        """
        self.cur.execute(create_ratings_table)
        self.cur.execute(create_credits_table)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
