import sys 
import yaml

def agregar_workers(yaml, tipo, cantidad):
    for i in range(1, int(cantidad) + 1):
        nombre = f"{tipo}{i}"
        yaml["services"][nombre] = {
            "container_name": nombre,
            "image": f"{tipo}:latest",
            "entrypoint": f"python3 /{tipo}.py",
            "environment": [
                f"WORKER_ID={i}",
                f"WORKER_TYPE={tipo.upper()}"
            ],
            "networks": ["processing_net"],
            "depends_on": ["orchestrator"]
        }

def generar_yaml(cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    """Genera un docker-compose.yaml"""
    yaml = {
        "name": "tp0",
        "services": {
            "server": {
                "container_name": "server",
                "image": "server:latest",
                "entrypoint": "python3 /main.py",
                "environment": [
                    "PYTHONUNBUFFERED=1",
                ],
                "networks": ["testing_net"],
                "volumes": [
                    "./server/config.ini:/config.ini",
                    #"./bets.csv:/bets.csv" # Descomentar para debuggear viendo el archivo
                ]
            }
        },
        "networks": {
            "testing_net": {
                "ipam": {
                    "driver": "default",
                    "config": [{"subnet": "172.25.125.0/24"}]
                }
            }
        }
    }

    agregar_workers(yaml, "filter", cant_filter)
    agregar_workers(yaml, "joiner", cant_joiner)
    agregar_workers(yaml, "aggregator", cant_aggregator)
    agregar_workers(yaml, "pnl", cant_pnl)

    return yaml
   
   


def generar_docker_compose(nombre_archivo, cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    compose = generar_yaml(cant_filter, cant_joiner, cant_aggregator, cant_pnl)
    with open(nombre_archivo, "w") as archivo:
        yaml.dump(compose, archivo, sort_keys=False)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        # Solo nombre del archivo -> usar 1 por defecto
        archivo_salida = sys.argv[1]
        filters = joiners = aggregators = pnls = 1
    elif len(sys.argv) == 6:
        _, archivo_salida, filters, joiners, aggregators, pnls = sys.argv
    else:
        print("Uso: python3 mi-generador.py <archivo_salida> [filters joiners aggregators pnls]")
        sys.exit(1)

    generar_docker_compose(archivo_salida, filters, joiners, aggregators, pnls)