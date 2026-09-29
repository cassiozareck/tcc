"""
Simulador de frota (versão Kafka): cada caminhão publica posições no tópico
consumido pelo consumer. As coordenadas são sorteadas e não representam rotas
reais, já que o processamento é emulado.
"""

import asyncio
import json
import os
import random
import time

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")  # 9094 = listener do host; dentro do compose usa kafka:9092
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "truck-data")

# vetor com as quantidades de caminhões testadas, uma rodada por valor
FLEET_SIZES = [int(n) for n in os.getenv("FLEET_SIZES", "50,75,100,125,150,200,250").split(",")]
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "10"))  # intervalo entre envios de cada caminhão
SENDS_PER_TRUCK = 6                                 # quantas mensagens cada caminhão manda

# Região (RS, PR e SC) onde as coordenadas são sorteadas
LAT_RANGE = (-33.0, -22.5)
LON_RANGE = (-58.0, -48.0)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

delivered = 0  # mensagens confirmadas pelo broker na rodada atual
failed = 0     # mensagens que o broker não confirmou


def on_delivery(err, msg):
    """Callback chamado pelo producer quando o broker confirma (ou rejeita) uma mensagem."""
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1


async def run_truck(truck_id, start_delay):
    """Publica SENDS_PER_TRUCK posições sem esperar a confirmação de cada uma."""
    await asyncio.sleep(start_delay)  # evita todos iniciarem ao mesmo instante

    for _ in range(SENDS_PER_TRUCK):
        payload = {
            "truck_id": truck_id,
            "latitude": random.uniform(*LAT_RANGE),
            "longitude": random.uniform(*LON_RANGE),
            "dest_lat": random.uniform(*LAT_RANGE),
            "dest_long": random.uniform(*LON_RANGE),
        }

        # publica e segue o ciclo sem esperar.
        # A chave é o truck_id: todas as posições de um caminhão vão pra mesma partição, em ordem
        producer.produce(KAFKA_TOPIC, key=str(truck_id), value=json.dumps(payload), on_delivery=on_delivery)
        producer.poll(0)  # atende os callbacks de confirmação pendentes

        await asyncio.sleep(SEND_INTERVAL_SECONDS)


async def run_test(num_trucks):
    """Roda uma rodada de simulação com num_trucks caminhões e devolve as métricas coletadas."""
    global delivered, failed
    delivered = failed = 0

    print(f"\n=== Rodada com {num_trucks} caminhões ===")

    tasks = [
        run_truck(truck_id, start_delay=random.uniform(0, SEND_INTERVAL_SECONDS))
        for truck_id in range(1, num_trucks + 1)
    ]

    start = time.perf_counter()
    await asyncio.gather(*tasks)  # espera terminar o envio para medir métricas

    print("Todos os caminhões terminaram de enviar, aguardando o Kafka confirmar tudo...")
    await asyncio.to_thread(producer.flush)  # bloqueia até todas as mensagens serem confirmadas
    elapsed = time.perf_counter() - start
    total = delivered + failed
    print(f"{delivered}/{total} mensagens confirmadas pelo Kafka")
    print(f"Tempo total até a última confirmação: {elapsed:.2f}s")

    return {"num_trucks": num_trucks, "ok": delivered, "total": total, "elapsed": elapsed}


async def main():
    summary = [await run_test(num_trucks) for num_trucks in FLEET_SIZES]

    print("\n=== Resumo ===")
    for r in summary:
        print(f"{r['num_trucks']} caminhões -> {r['elapsed']:.2f}s ({r['ok']}/{r['total']} ok)")


if __name__ == "__main__":
    asyncio.run(main())
