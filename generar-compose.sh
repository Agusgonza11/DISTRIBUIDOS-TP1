#!/bin/bash
echo "Cantidad de clientes: $1"
echo "Cantidad de workers filter: $2"
echo "Cantidad de workers joiner: $3"
echo "Cantidad de workers PNL: $4"
echo "Cantidad de workers aggregator: $5"
python3 mi-generador.py $1 $2 $3 $4 $5