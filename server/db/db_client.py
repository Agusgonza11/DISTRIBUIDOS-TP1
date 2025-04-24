import psycopg2 # type: ignore
import logging
from io import StringIO

from common.utils import create_dataframe, dictionary_to_list, list_to_string

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
        if nombre_tabla == "credits":
            dataframe = create_dataframe(datos_csv)
            dataframe.columns = ['id', 'cast']
            dataframe['cast'] = dataframe['cast'].apply(lambda x: list_to_string(dictionary_to_list(x)))
            datos_csv = dataframe.to_csv(index=False, header=False)
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
            CREATE TABLE IF NOT EXISTS ratings (
                movie_id INTEGER,
                rating FLOAT
            );
        """
        create_credits_table = """
            CREATE TABLE IF NOT EXISTS credits (
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
