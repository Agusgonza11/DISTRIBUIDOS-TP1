import os
import sys

ESPERADOS = ['1.txt', '2.txt', '3.txt', '4.txt', '5.txt']


def comparar_archivos(correctos, verificados, cliente):
    if len(correctos) != len(verificados):
        print("Error: las listas de archivos deben tener la misma cantidad.")
        return

    todos_iguales = True

    for i in range(len(correctos)):
        archivo_ref = os.path.join("client", "data", verificados[i])
        archivo_comp = os.path.join("resultados", correctos[i])
        iguales = True

        try:
            with open(archivo_ref, 'r') as ref, open(archivo_comp, 'r') as comp:
                lineas_ref = ref.readlines()
                lineas_comp = comp.readlines()
        except FileNotFoundError as e:
            print(f"\n❌ No se pudo abrir uno de los archivos:\n  {e.filename} no existe.")
            todos_iguales = False
            continue  # Saltea la comparación de este par y sigue con el siguiente


        max_lineas = max(len(lineas_ref), len(lineas_comp))

        for j in range(max_lineas):
            ref_linea = lineas_ref[j].strip() if j < len(lineas_ref) else "<Línea faltante>"
            comp_linea = lineas_comp[j].strip() if j < len(lineas_comp) else "<Línea faltante>"

            if ref_linea != comp_linea:
                if iguales:
                    print(f"\nDiferencias encontradas entre '{archivo_ref}' y '{archivo_comp}':")
                    iguales = False
                    todos_iguales = False
                print(f"  Línea {j+1}:\n    Esperado: '{ref_linea}'\n    Obtenido: '{comp_linea}'")

        if iguales:
            print(f"'{archivo_ref}' y '{archivo_comp}' son iguales.")

    if todos_iguales:
        print(f"\n✅ Todos los archivos comparados son iguales para el cliente {cliente}")
    else:
        print(f"\n❌ Se encontraron diferencias en algunos archivos para el cliente {cliente}")


def cargar_archivos(numero_cliente):
    return [
        f"{numero_cliente}_results_ARGENTINIAN-SPANISH-PRODUCTIONS.txt",
        f"{numero_cliente}_results_TOP-INVESTING-COUNTRIES.txt",
        f"{numero_cliente}_results_TOP-ARGENTINIAN-MOVIES-BY-RATING.txt",
        f"{numero_cliente}_results_TOP-ARGENTINIAN-ACTORS.txt",
        f"{numero_cliente}_results_SENTIMENT-ANALYSIS.txt"
    ]


def comparar(clients):
    for i in range(1, int(clients) + 1):
        archivos_cliente = cargar_archivos(i)
        comparar_archivos(ESPERADOS, archivos_cliente, i)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        _, clients = sys.argv
    else:
        print("Uso: python3 comparador-resultados.py [cant_clientes]")
        sys.exit(1)

    comparar(clients)