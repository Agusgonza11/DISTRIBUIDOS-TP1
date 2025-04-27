#!/bin/bash
echo "Cantidad de workers filter: $1"
echo "Cantidad de workers joiner: $2"
echo "Cantidad de workers PNL: $3"
echo "Cantidad de workers aggregator: $4"
python3 mi-generador.py "docker-compose-dev.yaml" $1 $2 $3 $4