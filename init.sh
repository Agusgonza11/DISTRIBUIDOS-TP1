#!/bin/bash

make docker-compose-down && make docker-compose-up

CONTAINERS=(
    "client_1"
    "joiner1"
    "input_gateway"
    "output_gateway"
    "broker"
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