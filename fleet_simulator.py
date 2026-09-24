"""
Protótipo de simulação de frota: caminhões saindo de cidades do RS com
destino a municípios do PR, enviando posições pro processing-engine (que já
calcula o ETA e roda o map matching sozinho).
"""

import asyncio
import math
import random
import time

import requests

PROCESS_ENGINE_URL = "http://localhost:8000/process"

NUM_TRUCKS = 2              # quantos caminhões simular (usa os N primeiros pares origem/destino)
SEND_INTERVAL_SECONDS = 5   # intervalo entre envios de cada caminhão
STEPS = 2                  # quantas posições cada caminhão manda
STEP_SIZE = 0.001           # avanço em graus por passo (~100m), rumo ao destino

# Cidades de origem no RS (nome, lat, lon)
ORIGENS = [
    ("Porto Alegre", -30.0346, -51.2177),
    ("Caxias do Sul", -29.1634, -51.1797),
    ("Canoas", -29.9177, -51.1830),
    ("Pelotas", -31.7654, -52.3376),
    ("Santa Maria", -29.6842, -53.8069),
    ("Novo Hamburgo", -29.6783, -51.1300),
    ("Gravataí", -29.9442, -50.9922),
    ("Passo Fundo", -28.2628, -52.4067),
    ("Bagé", -31.3313, -54.1069),
    ("Uruguaiana", -29.7547, -57.0883),
]

# Municípios de destino no PR (nome, lat, lon)
DESTINOS = [
    ("Curitiba", -25.4284, -49.2733),
    ("Londrina", -23.3103, -51.1628),
    ("Maringá", -23.4205, -51.9333),
    ("Ponta Grossa", -25.0916, -50.1668),
    ("Cascavel", -24.9555, -53.4552),
    ("Foz do Iguaçu", -25.5478, -54.5882),
    ("Guarapuava", -25.3907, -51.4622),
    ("Paranaguá", -25.5161, -48.5225),
    ("Umuarama", -23.7663, -53.3155),
    ("Toledo", -24.7246, -53.7412),
]


sent_tasks = []  # tarefas de envio disparadas, guardadas pra sabermos quando o processing-engine responder tudo


async def run_truck(truck_id, origin, dest, start_delay):
    """Anda em linha reta da origem rumo ao destino, disparando cada posição sem esperar a resposta."""
    await asyncio.sleep(start_delay) # evita todos iniciarem ao mesmo instante

    _, lat, lon = origin
    _, lat_d, lon_d = dest
    label = f"[Caminhão {truck_id} | {origin[0]} -> {dest[0]}]"

    # cálculo para se aproximar do destino
    dist = math.hypot(lat_d - lat, lon_d - lon)
    step_lat = (lat_d - lat) / dist * STEP_SIZE
    step_lon = (lon_d - lon) / dist * STEP_SIZE

     # envia localização STEPS vezes
    for step in range(1, STEPS + 1):
        lat += step_lat
        lon += step_lon

        payload = {
            "truck_id": truck_id,
            "latitude": lat,
            "longitude": lon,
            "dest_lat": lat_d,
            "dest_long": lon_d,
        }

        # dispara e segue o ciclo sem esperar a resposta, como um rastreador real faria
        sent_tasks.append(asyncio.create_task(asyncio.to_thread(requests.post, PROCESS_ENGINE_URL, json=payload)))
        print(f"{label} enviado")

        await asyncio.sleep(SEND_INTERVAL_SECONDS)

    print(f"{label} chegou")


async def main():
    print(f"Tempo esperado para finalizar: ~{SEND_INTERVAL_SECONDS * STEPS} - {SEND_INTERVAL_SECONDS * STEPS + 5}s")

    # sorteia origem/destino por caminhão (permite NUM_TRUCKS > 10)
    pairs = [(random.choice(ORIGENS), random.choice(DESTINOS)) for _ in range(NUM_TRUCKS)]

    # para cada caminhao aloca a função run truck para rodar em background, iniciando em intervalos
    # distintos para simular a frota mais realísticamente
    tasks = [
        run_truck(truck_id, origin, dest, start_delay=random.uniform(0, SEND_INTERVAL_SECONDS))
        for truck_id, (origin, dest) in enumerate(pairs, start=1)
    ]

    start = time.perf_counter()
    await asyncio.gather(*tasks) # espera terminar o processamento para medir métricas

    print(f"\nTodos os caminhões terminaram de enviar, aguardando o processing-engine responder tudo...")
    results = await asyncio.gather(*sent_tasks, return_exceptions=True)
    ok = sum(1 for r in results if not isinstance(r, Exception) and r.status_code == 200)
    print(f"{ok}/{len(results)} requisições respondidas com sucesso")
    print(f"Tempo total até a última resposta do processing-engine: {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
