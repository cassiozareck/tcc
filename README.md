# TCC — Engine de rastreamento de caminhões

API em FastAPI (`processing-engine.py`) que calcula o ETA de um caminhão até o
destino e, a cada 3 posições recebidas, roda Map Matching no GraphHopper para
descobrir em qual rua o caminhão está.

## Como rodar

### 1. Subir o GraphHopper (mapa do Sul)

Já existe um container chamado `graphhopper` com o grafo pré-processado em
`graph-hopper/default-gh`. Basta iniciar:

```bash
docker start graphhopper
```

Se o container não existir (primeira vez), crie assim:

```bash
docker run -d --name graphhopper -p 8989:8989 \
  -v /home/cassio/src/tcc/graph-hopper:/data \
  israelhikingmap/graphhopper:latest --config /data/config.yml
```

### 2. Subir a API

```bash
cd /home/cassio/src/tcc
.venv/bin/uvicorn processing-engine:app --port 8000
```

A API sobe em `http://localhost:8000` e expõe o endpoint `POST /process`.

## Testando com curl

O map matching só roda a partir da 3ª posição recebida para o mesmo
`truck_id` (janela deslizante das últimas 3 posições). Use o mesmo
`truck_id` nas 3 chamadas para ver o `street_name` sair preenchido na
terceira.

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
```

Resposta esperada (1ª e 2ª chamadas com `street_name` nulo, 3ª preenchido):
                        
```json
{"truck_id":7,"eta":29928221,"street_name":null}
{"truck_id":7,"eta":29939501,"street_name":null}
{"truck_id":7,"eta":29983850,"street_name":"Rua Alberto Torres"}
```

Use um `truck_id` diferente para reiniciar o histórico de posições do zero
(ele fica em memória, dentro do processo da API).

## Rodando o simulador de frota

Com o GraphHopper e a API já no ar (passos 1 e 2 acima), o `fleet_simulator.py`
simula caminhões saindo de cidades do RS com destino a municípios do PR,
enviando posições reais para o `processing-engine` a cada `SEND_INTERVAL_SECONDS`
segundos:

```bash
cd /home/cassio/src/tcc
.venv/bin/python fleet_simulator.py
```

Ajuste no topo do arquivo:
- `NUM_TRUCKS` — quantos caminhões simular (usa os N primeiros pares origem/destino).
- `SEND_INTERVAL_SECONDS` — intervalo entre envios de cada caminhão.
- `STEPS` — quantas posições cada caminhão manda antes de "chegar".

## Como matar os processos

```bash
# API (uvicorn)
pkill -f "uvicorn processing-engine"

# GraphHopper
docker stop graphhopper
```
