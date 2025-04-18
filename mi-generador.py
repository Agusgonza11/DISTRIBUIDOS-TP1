import sys
import yaml


def distribuir_consultas_por_nodo(cant_filter=1, cant_joiner=1, cant_aggregator=1, cant_pnl=1):
    consultas_por_tipo = {
        "filter": [1, 2, 3, 4, 5],
        "joiner": [3, 4],
        "aggregator": [2, 3, 4, 5],
        "pnl": [5]
    }

    # Diccionario que almacenará la distribución de consultas por nodo
    distribucion = {
        "filter": {},
        "joiner": {},
        "aggregator": {},
        "pnl": {}
    }

    # Asignar consultas a los nodos para cada tipo de worker
    for tipo, cantidad in {
        "filter": cant_filter,
        "joiner": cant_joiner,
        "aggregator": cant_aggregator,
        "pnl": cant_pnl
    }.items():
        consultas = consultas_por_tipo[tipo]
        cantidad = int(cantidad)
        
        # Crear un diccionario de asignación para los nodos
        asignacion = {i: [] for i in range(1, cantidad + 1)}
        
        # Asignar consultas a los nodos
        for i, consulta in enumerate(consultas):
            # Hacer la asignación balanceada: si hay más nodos que consultas, se repiten las consultas
            idx = (i % cantidad) + 1
            asignacion[idx].append(consulta)
        
        # Si hay más nodos que consultas, se repiten las consultas para balancear
        if cantidad > len(consultas):
            extras = cantidad - len(consultas)
            for i in range(extras):
                idx = ((i + len(consultas)) % cantidad) + 1
                consulta_idx = i % len(consultas)
                consulta = consultas[consulta_idx]
                asignacion[idx].append(consulta)

        # Guardar la asignación de consultas por tipo
        distribucion[tipo] = asignacion

    return distribucion


def calcular_eofs(tipo, distribucion):
    """
    Devuelve el número de EOF_ESPERADOS para el tipo de worker dado según la distribución de consultas.
    """
    eof_dict = {}

    if tipo == "pnl":
        # Para "pnl", devolvemos la cantidad de "filter" asignados a la consulta 5
        consulta = 5
        count = sum(1 for consultas in distribucion["filter"].values() if consulta in consultas)
        eof_dict[consulta] = count

    elif tipo == "joiner":
        # Para "joiner", devolvemos la cantidad de "filter" asignados a la consulta 3 y a la consulta 4
        consultas = [3, 4]
        for consulta in consultas:
            count = sum(1 for consultas in distribucion["filter"].values() if consulta in consultas)
            eof_dict[consulta] = count

    elif tipo == "aggregator":
        # Para "aggregator", devolvemos el número de workers de tipo "filter" asignados a consulta 2,
        # el número de workers de tipo "joiner" asignados a consulta 3, 4, y el número de workers de tipo "pnl" asignados a consulta 5.
        consultas = {
            2: "filter", 
            3: "joiner", 
            4: "joiner", 
            5: "pnl"
        }
        
        for consulta, tipo_consulta in consultas.items():
            if tipo_consulta == "filter":
                count = sum(1 for consultas in distribucion["filter"].values() if consulta in consultas)
            elif tipo_consulta == "joiner":
                count = sum(1 for consultas in distribucion["joiner"].values() if consulta in consultas)
            elif tipo_consulta == "pnl":
                count = sum(1 for consultas in distribucion["pnl"].values() if consulta in consultas)
            eof_dict[consulta] = count

    return eof_dict



def agregar_workers(compose, cant_filter=1, cant_joiner=1, cant_aggregator=1, cant_pnl=1):
    # Distribuir consultas por tipo de nodo
    consultas_por_nodo = distribuir_consultas_por_nodo(cant_filter, cant_joiner, cant_aggregator, cant_pnl)

    # Calcular EOF esperados por tipo
    eof_joiner = calcular_eofs("joiner", consultas_por_nodo)
    eof_pnl = calcular_eofs("pnl", consultas_por_nodo)
    eof_aggregator = calcular_eofs("aggregator", consultas_por_nodo)

    tipos = {
        "filter": cant_filter,
        "joiner": cant_joiner,
        "aggregator": cant_aggregator,
        "pnl": cant_pnl
    }

    for tipo, cantidad in tipos.items():
        for i in range(1, cantidad + 1):
            nombre = f"{tipo}{i}"
            consultas = consultas_por_nodo[tipo].get(i, [])
            env = [
                f"WORKER_ID={i}",
                f"WORKER_TYPE={tipo.upper()}",
                f"CONSULTAS={','.join(map(str, consultas))}"
            ]

            # EOF_ESPERADOS solo para ciertos tipos
            if tipo == "joiner":
                eof_str = ",".join(f"{k}:{v}" for k, v in eof_joiner.items())
                env.append(f"EOF_ESPERADOS={eof_str}")
            elif tipo == "pnl":
                eof_str = ",".join(f"{k}:{v}" for k, v in eof_pnl.items())
                env.append(f"EOF_ESPERADOS={eof_str}")
            elif tipo == "aggregator":
                eof_str = ",".join(f"{k}:{v}" for k, v in eof_aggregator.items())
                env.append(f"EOF_ESPERADOS={eof_str}")

            # EOF_ENVIAR para filters que atienden la consulta 5
            if tipo == "filter" and 5 in consultas:
                env.append(f"EOF_ENVIAR={cant_pnl}")

            # Agregar al compose
            compose["services"][nombre] = {
                "container_name": nombre,
                "image": f"{tipo}:latest",
                "entrypoint": f"python3 workers/{tipo}.py",
                "environment": env,
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

    agregar_workers(compose, cant_filter, cant_joiner, cant_aggregator, cant_pnl)

    return compose

def generar_docker_compose(nombre_archivo, cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    compose = generar_yaml(int(cant_filter), int(cant_joiner), int(cant_aggregator), int(cant_pnl))
    with open(nombre_archivo, "w") as archivo:
        yaml.dump(compose, archivo, sort_keys=False)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        archivo_salida = sys.argv[1]
        filters = joiners = aggregators = pnls = 1
    elif len(sys.argv) == 6:
        _, archivo_salida, filters, joiners, aggregators, pnls = sys.argv
        if int(aggregators) > 4:
            aggregators = '4'
    else:
        print("Uso: python3 mi-generador.py <archivo_salida> [filters joiners aggregators pnls]")
        sys.exit(1)

    generar_docker_compose(archivo_salida, filters, joiners, aggregators, pnls)
