"""
Protótipo de simulação de frota: caminhões saindo e chegando em cidades do
RS, PR e SC (origem/destino sorteados entre si), enviando posições pro
processing-engine (que já calcula o ETA e roda o map matching sozinho).
"""

import asyncio
import math
import random
import time

import requests

PROCESS_ENGINE_URL = "http://localhost:8000/process"
PROCESS_ENGINE_RESET_URL = "http://localhost:8000/reset"

FLEET_SIZES = [50, 75, 100, 125, 150, 200, 250]     # vetor com as quantidades de caminhões testadas, uma rodada por valor
SEND_INTERVAL_SECONDS = 10                          # intervalo entre envios de cada caminhão
STEPS = 6                                           # quantas posições cada caminhão manda
STEP_SIZE = 0.001                                   # avanço em graus por passo (~100m), rumo ao destino

# Cidades do RS, PR e SC (nome, lat, lon) usadas para sortear origem/destino
CIDADES = [
    # RS
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
    # PR
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
    # SC
    ("Florianópolis", -27.5954, -48.5480),
    ("Joinville", -26.3044, -48.8487),
    ("Blumenau", -26.9194, -49.0661),
    ("Chapecó", -27.1000, -52.6152),
    ("Criciúma", -28.6775, -49.3697),
    ("Itajaí", -26.9078, -48.6619),
    ("Lages", -27.8167, -50.3264),
    ("Balneário Camboriú", -26.9906, -48.6349),
    ("São José", -27.5969, -48.6394),
    ("Jaraguá do Sul", -26.4863, -49.0672),
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


async def run_test(num_trucks):
    """Roda uma rodada de simulação com num_trucks caminhões e devolve as métricas coletadas."""
    sent_tasks.clear()  # descarta tarefas de envio da rodada anterior

    # zera o estado acumulado do processing-engine pra não misturar localização com a rodada anterior
    await asyncio.to_thread(requests.post, PROCESS_ENGINE_RESET_URL)

    print(f"\n=== Rodada com {num_trucks} caminhões ===")
    print(f"Tempo máximo de processamento: {SEND_INTERVAL_SECONDS * STEPS + SEND_INTERVAL_SECONDS}s")

    # sorteia origem/destino por caminhão, sem repetir cidade dentro do par
    pairs = [tuple(random.sample(CIDADES, 2)) for _ in range(num_trucks)]

    # para cada caminhão aloca a função run truck para rodar em background, iniciando em intervalos
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
    elapsed = time.perf_counter() - start
    print(f"{ok}/{len(results)} requisições respondidas com sucesso")
    print(f"Tempo total até a última resposta do processing-engine: {elapsed:.2f}s")

    return {"num_trucks": num_trucks, "ok": ok, "total": len(results), "elapsed": elapsed}


async def main():
    summary = [await run_test(num_trucks) for num_trucks in FLEET_SIZES]

    print("\n=== Resumo ===")
    for r in summary:
        print(f"{r['num_trucks']} caminhões -> {r['elapsed']:.2f}s ({r['ok']}/{r['total']} ok)")


if __name__ == "__main__":
    asyncio.run(main())
