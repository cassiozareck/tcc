"""Consumidor Kafka que processa as coordenadas enviadas pela frota de caminhões.

Cada mensagem do tópico é um JSON com a posição atual e o destino de um caminhão.
Várias instâncias com o mesmo `group.id` dividem as partições do tópico entre si.
"""
import os
import time

from confluent_kafka import Consumer
from pydantic import BaseModel


class TruckData(BaseModel):
    """Estrutura da mensagem recebida da frota."""
    truck_id: int
    latitude: float
    longitude: float
    dest_lat: float
    dest_long: float


# Endereço do broker e tópico vêm do ambiente (ver docker-compose.yml)
consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
    # Consumidores do mesmo grupo repartem as partições; cada mensagem é processada uma vez
    'group.id': 'processing-engine',
    # Sem offset salvo para o grupo, começa do início do tópico (não perde mensagens antigas)
    'auto.offset.reset': 'earliest',
    # Commit do offset a cada 100ms (default 5s): o lag medido de fora erra no máximo ~1 mensagem por partição
    'auto.commit.interval.ms': 100,
})
consumer.subscribe([os.getenv('KAFKA_TOPIC', 'truck-data')])

processed = 0  # mensagens processadas por esta instância

while True:
    # Espera até 1s por uma mensagem; None significa que nada chegou
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error(), flush=True)
        continue

    truck_data = TruckData.model_validate_json(msg.value())
    # Tempo médio do processamento emulado
    time.sleep(0.092)
    processed += 1
    if processed % 50 == 0:
        print(f'{processed} coordenadas processadas (partição {msg.partition()}).', flush=True)
