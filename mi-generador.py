import sys
import yaml

def distribuir_consultas(tipo, cantidad):
    """Devuelve un diccionario: {nodo_index: [consultas_asignadas]}"""
    if tipo == "filter":
        consultas = [1, 2, 3, 4, 5]
    elif tipo == "joiner":
        consultas = [3, 4]
    elif tipo == "aggregator":
        consultas = [1, 2, 3, 4, 5]
    elif tipo == "pnl":
        consultas = [5]
    else:
        return {}

    cantidad = int(cantidad)
    asignacion = {}

    if len(consultas) <= cantidad:
        for i in range(1, cantidad + 1):
            asignacion[i] = consultas.copy()
    else:
        asignacion = {i: [] for i in range(1, cantidad + 1)}
        for i, consulta in enumerate(consultas):
            idx = (i % cantidad) + 1
            asignacion[idx].append(consulta)

    return asignacion


def agregar_workers(compose, tipo, cantidad):
    consultas_por_nodo = distribuir_consultas(tipo, int(cantidad))
    for i in range(1, int(cantidad) + 1):
        nombre = f"{tipo}{i}"
        consultas = consultas_por_nodo.get(i, [])
        compose["services"][nombre] = {
            "container_name": nombre,
            "image": f"{tipo}:latest",
            "entrypoint": f"python3 workers/{tipo}.py",
            "environment": [
                f"WORKER_ID={i}",
                f"WORKER_TYPE={tipo.upper()}",
                f"CONSULTAS={','.join(map(str, consultas))}"
            ],
            "networks": ["testing_net"],
            "depends_on": ["server", "rabbitmq"]
        }

def generar_yaml(cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    """Genera un docker-compose.yaml"""
    compose = {
        "name": "tp0",
        "services": {
            "rabbitmq": {
                "container_name": "rabbitmq",
                "image": "rabbitmq:management",
                "ports": ["15672:15672"],
                "networks": ["testing_net"]
            },
            "server": {
                "container_name": "server",
                "image": "server:latest",
                "entrypoint": "python3 /main.py",
                "environment": [
                    "PYTHONUNBUFFERED=1",
                    "LOGGING_LEVEL=DEBUG"
                ],
                "networks": ["testing_net"]
            },
            "client": {
                "container_name": "client",
                "image": "client:latest",
                "entrypoint": "/client",
                "environment": [
                    "CLI_ID=1",
                    "CLI_LOG_LEVEL=DEBUG"
                ],
                "networks": ["testing_net"],
                "depends_on": ["server"]
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

    agregar_workers(compose, "filter", cant_filter)
    agregar_workers(compose, "joiner", cant_joiner)
    agregar_workers(compose, "aggregator", cant_aggregator)
    agregar_workers(compose, "pnl", cant_pnl)

    return compose

def generar_docker_compose(nombre_archivo, cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    compose = generar_yaml(cant_filter, cant_joiner, cant_aggregator, cant_pnl)
    with open(nombre_archivo, "w") as archivo:
        yaml.dump(compose, archivo, sort_keys=False)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        archivo_salida = sys.argv[1]
        filters = joiners = aggregators = pnls = 1
    elif len(sys.argv) == 6:
        _, archivo_salida, filters, joiners, aggregators, pnls = sys.argv
    else:
        print("Uso: python3 mi-generador.py <archivo_salida> [filters joiners aggregators pnls]")
        sys.exit(1)

    generar_docker_compose(archivo_salida, filters, joiners, aggregators, pnls)
