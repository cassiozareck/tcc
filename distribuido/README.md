# Processing Engine — versão distribuída

## Como rodar

```bash
cd distribuido
docker compose up -d --build
```

Teste:

```bash
curl -X POST http://localhost:8080/process \
  -H 'Content-Type: application/json' \
  -d '{"truck_id":1,"latitude":-23.55,"longitude":-46.63,"dest_lat":-22.9,"dest_long":-43.17}'
```

Parar: `docker compose down`

## Observações

- Porta `8080` para não conflitar com `monoprocessado` (`8000`).
