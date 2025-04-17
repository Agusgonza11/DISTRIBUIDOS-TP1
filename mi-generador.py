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
    asignacion = {i: [] for i in range(1, cantidad + 1)}

    for i, consulta in enumerate(consultas):
        # Primer pase: asignar uno a uno
        idx = (i % cantidad) + 1
        asignacion[idx].append(consulta)

    # Segundo pase: si hay más nodos que consultas, replicar balanceado
    if cantidad > len(consultas):
        extras = cantidad - len(consultas)
        for i in range(extras):
            idx = ((i + len(consultas)) % cantidad) + 1
            consulta_idx = i % len(consultas)
            consulta = consultas[consulta_idx]
            asignacion[idx].append(consulta)

    return asignacion


def calcular_eofs_aggregator(cant_filter, cant_joiner, cant_pnl):
    origen = {
        2: "filter",
        3: "joiner",
        4: "joiner",
        5: "pnl"
    }

    distribuido = {
        "filter": distribuir_consultas("filter", cant_filter),
        "joiner": distribuir_consultas("joiner", cant_joiner),
        "pnl": distribuir_consultas("pnl", cant_pnl)
    }

    eof_por_consulta = {}

    for consulta in range(2, 6):
        tipo = origen[consulta]
        asignacion = distribuido[tipo]
        cuenta = sum(1 for consultas in asignacion.values() if consulta in consultas)
        eof_por_consulta[consulta] = cuenta

    return eof_por_consulta

def calcular_eofs_joiner(cant_filter):
    consultas_interes = [3, 4]
    asignacion = distribuir_consultas("filter", cant_filter)

    eof_por_consulta = {}
    for consulta in consultas_interes:
        cuenta = sum(1 for consultas in asignacion.values() if consulta in consultas)
        eof_por_consulta[consulta] = cuenta

    return eof_por_consulta


def agregar_workers(compose, tipo, cantidad, cant_filter=0, cant_joiner=0, cant_pnl=0):
    consultas_por_nodo = distribuir_consultas(tipo, int(cantidad))

    eof_str = ""
    if tipo == "aggregator":
        eof_dict = calcular_eofs_aggregator(int(cant_filter), int(cant_joiner), int(cant_pnl))
        eof_str = ",".join(f"{k}:{v}" for k, v in eof_dict.items())
    if tipo == "joiner":
        eof_dict = calcular_eofs_joiner(int(cant_filter))
        eof_str = ",".join(f"{k}:{v}" for k, v in eof_dict.items())

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

        if tipo == "aggregator":
            compose["services"][nombre]["environment"].append(f"EOF_ESPERADOS={eof_str}")
        if tipo == "joiner":
            compose["services"][nombre]["environment"].append(f"EOF_ESPERADOS={eof_str}")


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
    agregar_workers(compose, "joiner", cant_joiner, cant_filter)
    agregar_workers(compose, "aggregator", cant_aggregator, cant_filter, cant_joiner, cant_pnl)
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
        if int(aggregators) > 5:
            aggregators = '5'
    else:
        print("Uso: python3 mi-generador.py <archivo_salida> [filters joiners aggregators pnls]")
        sys.exit(1)

    generar_docker_compose(archivo_salida, filters, joiners, aggregators, pnls)
