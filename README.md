# TCC — Processamento de dados de rastreamento de frotas

Este repositório tem o código do TCC: um motor de processamento que recebe a
posição (latitude/longitude) de caminhões e calcula informações de
rastreamento, como o **ETA** (tempo estimado até o destino) e a **rua em que o
caminhão está** (via *map matching* no GraphHopper).

O trabalho **compara duas arquiteturas** para esse mesmo problema, observando
como cada uma se comporta conforme a frota cresce. Nas duas existe um
**simulador de frota** (`fleet-simulator.py`) que faz o papel dos caminhões,
enviando posições periodicamente para o motor.

| | Monoprocessado | Distribuído |
|---|---|---|
| Comunicação | HTTP síncrono | Mensageria assíncrona (Apache Kafka) |
| Processamento | Uma única instância da API (FastAPI) | Vários consumers em paralelo |
| Pasta | [`monoprocessado/`](monoprocessado/) | [`distribuido/`](distribuido/) |

## Estrutura do repositório

```
.
├── monoprocessado/   # versão HTTP + FastAPI + GraphHopper
└── distribuido/      # versão com Kafka (consumer/ e producer/)
```

Cada pasta é independente e tem o seu próprio README explicando a arquitetura
e como rodar:

- **[monoprocessado/README.md](monoprocessado/README.md)**
- **[distribuido/README.md](distribuido/README.md)**

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) com Docker Compose
- Python 3.12 e [uv](https://docs.astral.sh/uv/) (só para rodar o simulador
  monoprocessado fora do Docker)
