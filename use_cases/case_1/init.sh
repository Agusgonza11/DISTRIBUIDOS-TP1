#!/bin/bash

# Un unico cliente

# # # # 
# Inicializa la consulta, y luego se cae. El Gateway debería enviar los EOFs y dar por cerrada la consulta
# Se cae cualquier nodo, y sin hacer nada debería volver a levantarse (HC), y terminar la consulta exitosamente
# Se cae el Gateway, Y cuando se levanta envía todos los EOFs para terminar de procesar las consultas en curso
# 

../clean.sh

make docker-compose-up

CONTAINERS=(
    "client_1"
    "joiner1"
    "input_gateway"
    "output_gateway"
)

first=1
for NAME in "${CONTAINERS[@]}"
do
  if [ $first -eq 1 ]; then
    osascript <<END
tell application "Terminal"
    activate
    do script "echo LOGS DEL CONTENEDOR: $NAME; docker logs -f $NAME"
end tell
END
    first=0
  else
    osascript <<END
tell application "Terminal"
    activate
    tell application "System Events" to keystroke "t" using command down
    delay 0.5
    do script "echo LOGS DEL CONTENEDOR: $NAME; docker logs -f $NAME" in front window
end tell
END
  fi
done