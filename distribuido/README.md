# Processing Engine — versão distribuída (Kafka)

Um broker Kafka (KRaft) com o tópico `truck-data` em X partições, consumidores
que processam as mensagens (coordenadas) e um producer que simula a frota.

```
distribuido/
├── docker-compose.yml
├── consumer/   # processing-engine.py: consome o tópico e processa cada posição
└── producer/   # fleet-simulator.py: simula os caminhões e publica no tópico
```

## Como rodar

```bash
cd distribuido
docker compose down
docker compose up --build
```

Parar: `docker compose down`

## Simular a frota (producer)

```bash
docker compose run --rm --build producer
```

Roda uma rodada por valor de `FLEET_SIZES` (50, 75, ... 250 caminhões). Cada
caminhão publica com `truck_id` como chave, então suas posições ficam na mesma partição.
Antes de cada rodada espera o lag total chegar a 0, para o backlog da rodada anterior não interferir.
A cada 5s (`REPORT_INTERVAL_SECONDS`) imprime quantas coordenadas já foram enviadas e o lag total dos consumers, e ao fim de cada rodada o lag por partição, máximo e total.
Para um teste rápido: `docker compose run --rm --build -e FLEET_SIZES=3 -e SEND_INTERVAL_SECONDS=1 producer`

Ver o processamento: `docker compose logs -f consumer`

## Ver o consumer lag

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group processing-engine
```

`LAG` 0 em todas as partições significa que tudo o que foi publicado já foi consumido.

## Variáveis de ambiente

**`consumer`** (opcionais, já têm default no código):

| Variável | Default | Função |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | endereço do broker |
| `KAFKA_TOPIC` | `truck-data` | tópico consumido |

**`producer`**: `KAFKA_BOOTSTRAP` (default `localhost:9094`, no compose `kafka:9092`),
`KAFKA_TOPIC` (tópicos),
`FLEET_SIZES` (caminhões por rodada, separados por vírgula),
`SEND_INTERVAL_SECONDS` (intervalo entre envios de cada caminhão) e
`SENDS_PER_TRUCK` (mensagens/steps que cada caminhão envia por rodada, default 6),
`REPORT_INTERVAL_SECONDS` (intervalo do print de coordenadas enviadas e lag total, default 5).

**`kafka`**: modo KRaft (sem Zookeeper). As principais são `KAFKA_NUM_PARTITIONS`
(partições dos tópicos criados automaticamente) e `KAFKA_LISTENERS` /
`KAFKA_ADVERTISED_LISTENERS` (porta `9092` para os containers, `9094` para o host).
Os demais campos estão comentados no `docker-compose.yml`.
