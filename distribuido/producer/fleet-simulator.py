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

from confluent_kafka import Consumer, Producer, TopicPartition

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")  # 9094 = listener do host; dentro do compose usa kafka:9092
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "truck-data")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "processing-engine")  # grupo dos consumers, usado só para ler o lag

# vetor com as quantidades de caminhões testadas, uma rodada por valor
FLEET_SIZES = [int(n) for n in os.getenv("FLEET_SIZES", "50,75,100,125,150,200,250").split(",")]
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "10"))  # intervalo entre envios de cada caminhão
SENDS_PER_TRUCK = int(os.getenv("SENDS_PER_TRUCK", "6"))  # quantas mensagens (steps) cada caminhão manda

# Região (RS, PR e SC) onde as coordenadas são sorteadas
LAT_RANGE = (-33.0, -22.5)
LON_RANGE = (-58.0, -48.0)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Só consulta offsets: não faz subscribe, então não entra no grupo nem rouba partições dos consumers
lag_reader = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP,
    "enable.auto.commit": False,
})

delivered = 0  # mensagens confirmadas pelo broker na rodada atual
failed = 0     # mensagens que o broker não confirmou


def on_delivery(err, msg):
    """Callback chamado pelo producer quando o broker confirma (ou rejeita) uma mensagem."""
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1


def get_consumer_lag():
    """Devolve {partição: lag} do grupo dos consumers: fim do log menos o offset já commitado."""
    partitions = lag_reader.list_topics(KAFKA_TOPIC, timeout=10).topics[KAFKA_TOPIC].partitions
    committed = lag_reader.committed([TopicPartition(KAFKA_TOPIC, p) for p in partitions], timeout=10)

    lags = {}
    for tp in sorted(committed, key=lambda t: t.partition):
        low, high = lag_reader.get_watermark_offsets(tp, timeout=10)
        # offset negativo = grupo ainda sem commit nessa partição: tudo o que está no log é lag
        lags[tp.partition] = high - (tp.offset if tp.offset >= 0 else low)
    return lags


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

    # Backlog dos consumers no instante em que o envio terminou (o offset commitado atrasa até ~500ms)
    lags = await asyncio.to_thread(get_consumer_lag)
    max_lag = max(lags.values())
    print("Lag por partição: " + " ".join(f"p{p}={lag}" for p, lag in lags.items()))
    print(f"Lag máximo: {max_lag}")

    return {"num_trucks": num_trucks, "ok": delivered, "total": total, "elapsed": elapsed, "max_lag": max_lag}


async def main():
    summary = [await run_test(num_trucks) for num_trucks in FLEET_SIZES]

    print("\n=== Resumo ===")
    for r in summary:
        print(f"{r['num_trucks']} caminhões -> {r['elapsed']:.2f}s ({r['ok']}/{r['total']} ok), lag máximo {r['max_lag']}")


if __name__ == "__main__":
    asyncio.run(main())
