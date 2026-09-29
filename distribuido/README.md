# Processing Engine — versão distribuída (Kafka)

Um broker Kafka (KRaft) com o tópico `truck-data` em 5 partições, um consumidor
que processa cada mensagem e um producer que simula a frota.

```
distribuido/
├── docker-compose.yml
├── consumer/   # processing-engine.py: consome o tópico e processa cada posição
└── producer/   # fleet-simulator.py: simula os caminhões e publica no tópico
```

## Como rodar

```bash
cd distribuido
docker compose up -d --build
```

## Simular a frota (producer)

```bash
docker compose run --rm producer
```

Roda uma rodada por valor de `FLEET_SIZES` (50, 75, ... 250 caminhões). Cada
caminhão publica com `truck_id` como chave, então suas posições ficam na mesma partição.
Para um teste rápido: `docker compose run --rm -e FLEET_SIZES=3 -e SEND_INTERVAL_SECONDS=1 producer`

## Enviar uma mensagem manual

```bash
echo '{"truck_id":1,"latitude":-23.55,"longitude":-46.63,"dest_lat":-22.9,"dest_long":-43.17}' | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic truck-data
```

Ver o processamento: `docker compose logs -f consumer`

De fora do Docker, o broker está em `localhost:9094`.

Parar: `docker compose down`

## Observações

- O tópico é criado automaticamente na primeira mensagem, com 5 partições (`KAFKA_NUM_PARTITIONS`).
- O número de consumers fica em `deploy.replicas` no `docker-compose.yml` (5, uma por partição).
  Consumers no mesmo grupo dividem as partições; acima de 5 ficam ociosos.
  Para mudar sem editar o arquivo: `docker compose up -d --scale consumer=3`

## Variáveis de ambiente

**`consumer`** (opcionais, já têm default no código):

| Variável | Default | Função |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | endereço do broker |
| `KAFKA_TOPIC` | `truck-data` | tópico consumido |

**`producer`**: `KAFKA_BOOTSTRAP` (default `localhost:9094`, no compose `kafka:9092`),
`KAFKA_TOPIC`, `FLEET_SIZES` (caminhões por rodada, separados por vírgula) e
`SEND_INTERVAL_SECONDS` (intervalo entre envios de cada caminhão).

**`kafka`**: modo KRaft (sem Zookeeper). As principais são `KAFKA_NUM_PARTITIONS`
(partições dos tópicos criados automaticamente) e `KAFKA_LISTENERS` /
`KAFKA_ADVERTISED_LISTENERS` (porta `9092` para os containers, `9094` para o host).
Os demais campos estão comentados no `docker-compose.yml`.
