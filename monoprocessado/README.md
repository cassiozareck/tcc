# TCC — Engine de rastreamento de caminhões

API em FastAPI (`processing-engine.py`) que calcula o ETA de um caminhão até o
destino e, a cada 5 posições recebidas, roda Map Matching no GraphHopper para
descobrir em qual rua o caminhão está.

## Como rodar

### 1. Subir os serviços

O GraphHopper (mapa do Sul) e o `processing-engine` sobem juntos pelo Docker
Compose:

```bash
docker compose up -d
```

A API sobe em `http://localhost:8000` e expõe o endpoint `POST /process`.

Se o container `graphhopper` não existir, crie manualmente:

```bash
docker run -d --name graphhopper -p 8989:8989 \
  -v $(pwd)/graph-hopper:/data \
  israelhikingmap/graphhopper:latest --config /data/config.yml
```

## Testando com curl

O map matching roda a cada 5 posições recebidas para o mesmo `truck_id`
(as posições acumulam e a lista zera depois do match). Use o mesmo
`truck_id` nas 5 chamadas para ver o `street_name` sair preenchido na
quinta.

Exemplo: caminhão saindo de Porto Alegre com destino Curitiba.

```bash
curl -s -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{"truck_id": 7, "latitude": -30.0346, "longitude": -51.2177, "dest_lat": -25.4284, "dest_long": -49.2733}'

curl -s -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{"truck_id": 7, "latitude": -30.0350, "longitude": -51.2180, "dest_lat": -25.4284, "dest_long": -49.2733}'

curl -s -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{"truck_id": 7, "latitude": -30.0400, "longitude": -51.2200, "dest_lat": -25.4284, "dest_long": -49.2733}'

curl -s -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{"truck_id": 7, "latitude": -30.0430, "longitude": -51.2220, "dest_lat": -25.4284, "dest_long": -49.2733}'

curl -s -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{"truck_id": 7, "latitude": -30.0460, "longitude": -51.2240, "dest_lat": -25.4284, "dest_long": -49.2733}'
```

Resposta esperada (1ª a 4ª chamadas com `street_name` nulo, 5ª preenchido):
                        
```json
{"truck_id":7,"eta":29928221,"street_name":null}
{"truck_id":7,"eta":29939501,"street_name":null}
{"truck_id":7,"eta":29983850,"street_name":null}
{"truck_id":7,"eta":29995000,"street_name":null}
{"truck_id":7,"eta":30010000,"street_name":"Rua Alberto Torres"}
```

## Rodando o simulador de frota

Para rodar o simulador de frotas:

```bash
.venv/bin/python fleet-simulator.py
```

Ajuste no topo do arquivo:
- `NUM_TRUCKS` — quantos caminhões simular (usa os N primeiros pares origem/destino).
- `SEND_INTERVAL_SECONDS` — intervalo entre envios de cada caminhão.
- `STEPS` — quantas posições cada caminhão irá mandar

## Como matar os processos

```bash
docker compose down
```
