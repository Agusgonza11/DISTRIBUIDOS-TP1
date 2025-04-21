import asyncio
import aio_pika # type: ignore
import logging

QUEUE_NAME = "filter_consult_2"

async def enviar_mock():
    logging.basicConfig(level=logging.INFO)

    connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
    channel = await connection.channel()
    await channel.declare_queue(QUEUE_NAME, durable=True)

    # Formato exacto tipo CSV (como texto plano con saltos de línea)
    contenido_csv = (
        "id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue\n"
        "1,El secreto de sus ojos,\"['Crime', 'Drama', 'Mystery', 'Romance']\",2009-08-13,\"A retired legal counselor writes a novel hoping to find closure.\",\"['Argentina', 'Spain']\",\"['Spanish']\",2000000,6000000\n"
        "2,Toy Story,\"['Adventure', 'Animation', 'Children', 'Comedy', 'Fantasy']\",1995-11-22,\"A story of toys that come to life.\",\"['United States']\",\"['English']\",30000000,373554033\n"
        "3,Relatos salvajes,\"['Drama', 'Thriller']\",2014-08-21,\"Six short stories that explore extremities of human behavior.\",\"['Argentina', 'Spain']\",\"['Spanish']\",3400000,28000000\n"
        "4,Whisky,\"['Comedy', 'Drama', 'Foreign']\",2004-05-15,\"A quiet man runs a sock factory in Uruguay.\",\"['Argentina', 'Uruguay']\",\"['Spanish']\",1000000,2500000\n"
        "5,Tetro,\"['Drama', 'Mystery']\",2009-05-14,\"The story of two brothers, artists, and their estranged family.\",\"['Argentina', 'Spain']\",\"['English', 'Spanish']\",5000000,2600000\n"
    )

    await channel.default_exchange.publish(
        aio_pika.Message(
            body=contenido_csv.encode(),
            headers={"type": "CONSULT", "Query": 2, "ClientID": 1}
        ),
        routing_key=QUEUE_NAME,
    )
    logging.info("Enviado batch CSV con header type=CONSULT")

    # Enviar EOF con header type="EOF"
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=b"EOF",
            headers={"type": "EOF", "Query": 2, "ClientID": 1}
        ),
        routing_key=QUEUE_NAME,
    )
    logging.info("Enviado EOF")

    await connection.close()

