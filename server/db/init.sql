CREATE TABLE IF NOT EXISTS ratings (
    movie_id INTEGER PRIMARY KEY,
    rating FLOAT
);

CREATE TABLE IF NOT EXISTS credits (
    movie_id INTEGER PRIMARY KEY,
    cast_list TEXT[]
);
