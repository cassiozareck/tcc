import os
import time

from fastapi import FastAPI
from pydantic import BaseModel

# Estrutura de mensagem recebida da frota
class TruckData(BaseModel):
    truck_id: int
    latitude: float
    longitude: float
    dest_lat: float
    dest_long: float

app = FastAPI()

# Endpoint para processamento de uma coordenada unitária
@app.post("/process")
def process(truck_data: TruckData):
    # Tempo médio do processamento emulado
    time.sleep(0.092)
    return f'Localização do caminhão {truck_data.truck_id} processada.'