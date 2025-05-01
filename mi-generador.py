import sys
import yaml


def get_joiners_consultas_from_compose(compose):
    joiners = {}
    for service_name, service in compose.get('services', {}).items():
        if service_name.startswith('joiner'):
            env_vars = service.get('environment', [])
            worker_id = None
            consultas = None
            for env in env_vars:
                if env.startswith('WORKER_ID='):
                    worker_id = int(env.split('=')[1])
                if env.startswith('CONSULTAS='):
                    consultas = list(map(int, env.split('=')[1].split(',')))
            if worker_id is not None and consultas is not None:
                joiners[worker_id] = consultas
    return joiners



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


def calcular_eof_enviar_filters(consultas_por_nodo):
    resultado = {}
    
    joiners = consultas_por_nodo["joiner"]
    pnls = consultas_por_nodo["pnl"]

    # Consulta → cantidad de nodos que la atienden
    destinos = {
        3: sum(1 for consultas in joiners.values() if 3 in consultas),
        4: sum(1 for consultas in joiners.values() if 4 in consultas),
        5: sum(1 for consultas in pnls.values() if 5 in consultas)
    }

    filters = consultas_por_nodo["filter"]
    for filter_id, consultas in filters.items():
        filtros = {}
        for consulta in consultas:
            if consulta in destinos:
                filtros[consulta] = destinos[consulta]
        resultado[filter_id] = filtros

    return resultado


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

def agregar_broker(compose, cant_filter=1, cant_joiner=1, cant_aggregator=1, cant_pnl=1):
    consultas_por_nodo = distribuir_consultas_por_nodo(cant_filter, cant_joiner, cant_aggregator, cant_pnl)
    eof_aggregator = calcular_eofs("aggregator", consultas_por_nodo)
    eof_str_agg = ",".join(f"{k}:{v}" for k, v in eof_aggregator.items())
    eof_joiner = calcular_eofs("joiner", consultas_por_nodo)
    cant_filters_pnl = calcular_eofs("pnl", consultas_por_nodo)
    eof_joiner.update(cant_filters_pnl)
    eof_str_joiner = ",".join(f"{k}:{v}" for k, v in eof_joiner.items())
    joiners = get_joiners_consultas_from_compose(compose)
    str_joiners = ";".join(f"{k}:{v}" for k, v in joiners.items())

    compose["services"]["broker"] = {
                    "container_name": "broker",
                    "image": "broker:latest",
                    "entrypoint": f"python3 workers/broker.py",
                    "environment": [
                        f"EOF_ESPERADOS={eof_str_joiner}",
                        f"EOF_ENVIAR={eof_str_agg}",
                        f"JOINERS={str_joiners}",
                        ],
                    "networks": ["testing_net"],
                    "depends_on": {
                            "rabbitmq": {"condition": "service_healthy"}
                    }
                }


def agregar_workers(compose, cant_filter=1, cant_joiner=1, cant_aggregator=1, cant_pnl=1):
    # Distribuir consultas por tipo de nodo
    consultas_por_nodo = distribuir_consultas_por_nodo(cant_filter, cant_joiner, cant_aggregator, cant_pnl)

    # Calcular EOF esperados por tipo
    eof_joiner = calcular_eofs("joiner", consultas_por_nodo)
    eof_pnl = calcular_eofs("pnl", consultas_por_nodo)
    eof_aggregator = calcular_eofs("aggregator", consultas_por_nodo)
    eof_enviar_por_filter = calcular_eof_enviar_filters(consultas_por_nodo)

    tipos = {
        "filter": cant_filter,
        "joiner": cant_joiner,
        "aggregator": cant_aggregator,
        "pnl": cant_pnl,
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


            # Agregar al compose
            compose["services"][nombre] = {
                "container_name": nombre,
                "image": f"{tipo}:latest",
                "entrypoint": f"python3 workers/{tipo}.py",
                "environment": env,
                "networks": ["testing_net"],
                "depends_on": {
                        "rabbitmq": {"condition": "service_healthy"}
                }
            }

def agregar_clientes(compose, cantidad_clientes):
    for i in range(1, int(cantidad_clientes) + 1):
        nombre_servicio = f"client_{i}"
        compose["services"][nombre_servicio] = {
            "container_name": nombre_servicio,
            "image": "client:latest",
            "entrypoint": "/client",
            "environment": [
                f"CLI_ID={i}",
                "CLI_LOG_LEVEL=DEBUG"
            ],
            "volumes": ["./client/data:/app/data"],
            "networks": ["testing_net"],
            "depends_on": ["input_gateway", "output_gateway"]
        }



def generar_yaml(clients, cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    """Genera un docker-compose.yaml"""
    consultas_por_nodo = distribuir_consultas_por_nodo(cant_filter, cant_joiner, cant_aggregator, cant_pnl)
    
    compose = {
        "name": "tp0",
        "services": {
            "rabbitmq": {
                "container_name": "rabbitmq",
                "image": "rabbitmq:3-management",
                "networks": ["testing_net"],
                "ports": ["15672:15672", "5672:5672"],
                "healthcheck": {
                    "test": "rabbitmq-diagnostics check_port_connectivity",
                    "interval": "5s",
                    "timeout": "3s",
                    "retries": 10,
                    "start_period": "50s"
                }
            },
            "input_gateway": {
                "container_name": "input_gateway",
                "image": "input_gateway:latest",
                "entrypoint": "/input_gateway",
                "environment": [
                    "CLI_LOG_LEVEL=DEBUG",
                    *construir_env_input_gateway(consultas_por_nodo)
                ],
                "ports": ["5000:5000", "5001:5001", "5002:5002"],
                "networks": ["testing_net"],
                "depends_on": {
                    "rabbitmq": {"condition": "service_healthy"}
                },
                "links": ["rabbitmq"]
            },
            "output_gateway": {
                "container_name": "output_gateway",
                "image": "output_gateway:latest",
                "entrypoint": "/output_gateway",
                "environment": [
                    "CLI_LOG_LEVEL=DEBUG",
                    *construir_env_output_gateway(consultas_por_nodo)
                ],
                "ports": ["6000:6000"],
                "networks": ["testing_net"],
                "depends_on": {
                    "rabbitmq": {"condition": "service_healthy"}
                },
                "links": ["rabbitmq"]
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

    agregar_clientes(compose, clients)
    agregar_workers(compose, cant_filter, cant_joiner, cant_aggregator, cant_pnl)
    agregar_broker(compose, cant_filter, cant_joiner, cant_aggregator, cant_pnl)
    return compose

def construir_env_input_gateway(consultas_por_nodo):
    archivo_por_consulta = {
        1: ["movies"],
        2: ["movies"],
        3: ["movies", "ratings"],
        4: ["movies", "credits"],
        5: ["movies"]
    }
    
    consultas_filter = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    consultas_join = {3: 0, 4: 0}
    
    for node, consultas in consultas_por_nodo["filter"].items():
        for consulta in consultas:
            consultas_filter[consulta] += 1
    for node, consultas in consultas_por_nodo["joiner"].items():
        for consulta in consultas:
            consultas_join[consulta] += 1

    envs = []
    
    for numero_consulta, cantidad_nodos in consultas_filter.items():
        nombre_env = "CONSULTA_" + str(numero_consulta) + "_FILTER"
        envs.append(f"{nombre_env}={cantidad_nodos}")
        
    for numero_consulta, cantidad_nodos in consultas_join.items():
        nombre_env = "CONSULTA_" + str(numero_consulta) + "_JOIN"
        envs.append(f"{nombre_env}={cantidad_nodos}")

    return envs


def construir_env_output_gateway(consultas_por_nodo):
    count_consulta_1 = 0
    
    for node, consultas in consultas_por_nodo["filter"].items():
        for consulta in consultas:
            if consulta == 1:
                count_consulta_1 += 1

    envs = []
    
    nombre_env = "CONSULTA_1_EOF_COUNT"
    envs.append(f"{nombre_env}={count_consulta_1}")

    return envs

def generar_docker_compose(clients, cant_filter, cant_joiner, cant_aggregator, cant_pnl):
    compose = generar_yaml(clients, int(cant_filter), int(cant_joiner), int(cant_aggregator), int(cant_pnl))
    with open("docker-compose-dev.yaml", "w") as archivo:
        yaml.dump(compose, archivo, sort_keys=False)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        clients = filters = joiners = aggregators = pnls = 1
    elif len(sys.argv) == 6:
        _, clients, filters, joiners, pnls, aggregators = sys.argv
        if int(aggregators) > 4:
            aggregators = '4'
    else:
        print("Uso: python3 mi-generador.py [clients filters joiners pnls aggregators]")
        sys.exit(1)

    generar_docker_compose(clients, filters, joiners, aggregators, pnls)
