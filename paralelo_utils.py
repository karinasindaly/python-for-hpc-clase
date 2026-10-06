import math
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor


def funcion(x):
    return math.sqrt(x) + x**2 + math.sin(x) + math.cos(x) + math.log(x)


def procesar_bloque(bloque):
    return [funcion(x) for x in bloque]


def ejecutar_paralelo(datos, workers):
    if workers < 1:
        raise ValueError("Debe haber al menos un worker.")

    inicio = time.perf_counter()

    # Repartir los datos en bloques conservando el orden.
    bloques = [
        datos[i * len(datos) // workers:
              (i + 1) * len(datos) // workers]
        for i in range(workers)
    ]

    contexto = mp.get_context("spawn")

    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=contexto
    ) as executor:
        resultados = [
            valor
            for bloque in executor.map(procesar_bloque, bloques)
            for valor in bloque
        ]

    tiempo = time.perf_counter() - inicio
    return resultados, tiempo
