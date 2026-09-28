import os

from fastapi import FastAPI
from pydantic import BaseModel
import requests

GRAPHHOPPER_URL = os.environ.get("GRAPHHOPPER_URL", "http://localhost:8989")

# Estrutura de mensagem recebida da frota
class TruckData(BaseModel):
    truck_id: int
    latitude: float
    longitude: float
    dest_lat: float
    dest_long: float

app = FastAPI()

trucks = {} # Guarda últimas 3 coordenadas dos caminhões

calls = 0

# Endpoint para limpar o estado acumulado (usado entre rodadas de teste)
@app.post("/reset")
def reset():
    trucks.clear()
    return {"status": "ok"}

# Endpoint para processamento de uma coordenada unitária
@app.post("/process")
def process(truck_data: TruckData):
    global calls
    calls += 1
    print(f"Chamadas: {calls}")

    # Inicialmente devemos calcular o ETA, colocamos no corpo da mensagem latitude e longitude origem e destino
    route_params = {
        "profile": "truck",
        "points_encoded": "false",
        "point": [
            f"{truck_data.latitude},{truck_data.longitude}",
            f"{truck_data.dest_lat},{truck_data.dest_long}",
        ]
    }

    resp = requests.get(f"{GRAPHHOPPER_URL}/route", params=route_params) # Envia para GraphHopper

    if resp.status_code != 200:
        raise Exception(f"Erro no cálculo do ETA: {resp.text}")

    route_paths = resp.json().get("paths")
    if not route_paths:
        raise Exception("Erro no cálculo do ETA: nenhuma rota retornada")

    eta = route_paths[0].get("time") # Aqui fica o tempo estimado segundo a documentação
    if eta is None:
        raise Exception("Erro no cálculo do ETA: propriedade 'time' ausente na resposta")

    # Temos que salvar as últimas coordenadas dos caminhões
    if truck_data.truck_id not in trucks:
        trucks[truck_data.truck_id] = []
    trucks[truck_data.truck_id].append((truck_data.latitude, truck_data.longitude))

    street_name = None

    # Agora realizamos o Map Matching a cada 3 posições acumuladas
    coords = trucks[truck_data.truck_id]

    if len(coords) == 5:
        # Limpa já aqui: se o match falhar, não fica preso pra sempre nessas
        # 5 posições, e o próximo ciclo começa a acumular do zero
        trucks[truck_data.truck_id] = []

        gpx_points = "".join(
            f'<trkpt lat="{lat}" lon="{lon}"></trkpt>\n' for lat, lon in coords
        )

        # Envia as coordenadas para o GraphHopper em um formato GPX, conforme documentação
        match_resp = requests.post(
            f"{GRAPHHOPPER_URL}/match",
            data=(
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                "<gpx>\n<trk>\n<trkseg>\n"
                f"{gpx_points}"
                "</trkseg>\n</trk>\n</gpx>"
            ),
            headers={"Content-Type": "application/gpx+xml"},
            params={
                "profile": "truck",
                "type": "json",
                "points_encoded": "false",
                "elevation": "false",
            },
        )

        if match_resp.status_code != 200:
            raise Exception(f"Erro no Map Matching: {match_resp.text}")

        match_paths = match_resp.json().get("paths")
        if not match_paths:
            raise Exception("Erro no Map Matching: nenhuma rota retornada")

        instructions = match_paths[0].get("instructions") # Fica aqui a rua
        if not instructions:
            raise Exception("Erro no Map Matching: nenhuma instrução retornada")

        # A rua atual do caminhão é a última com street_name preenchido
        # (alguns trechos do OSM não têm nome).
        street_names = [i["street_name"] for i in instructions if i.get("street_name")]
        if not street_names:
            raise Exception("Erro no Map Matching: nenhuma instrução com 'street_name' preenchido")
        street_name = street_names[-1]

    return {
        "truck_id": truck_data.truck_id,
        "eta": eta,
        "street_name": street_name,
    }