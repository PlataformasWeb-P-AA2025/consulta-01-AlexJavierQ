import pandas as pd
from pymongo import MongoClient

# 1. Conexión a MongoDB local
client = MongoClient("mongodb://localhost:27017/")
db = client["torneos_tenis"]
collection = db["partidos"]

# 2. Cargar archivos Excel
archivos = ["data/2022.xlsx", "data/2023.xlsx"]
dataframes = [pd.read_excel(archivo) for archivo in archivos]

# 3. Unir DataFrames y convertir a registros
df = pd.concat(dataframes, ignore_index=True)
registros = df.to_dict(orient="records")

# 4. Insertar en MongoDB
collection.delete_many({})  # Limpia antes de insertar
collection.insert_many(registros)
print(f"✅ Insertados {len(registros)} registros en MongoDB.")

# 5. Consultas

# Consulta 1: Mostrar los partidos jugados en Adelaide
print("\n📌 Consulta 1: Partidos jugados en Adelaide")
for partido in collection.find({"Location": "Adelaide"}).limit(5):
    print(f"{partido['Date']} - {partido['Winner']} vs {partido['Loser']}")

# Consulta 2: Cuántos partidos ganó cada jugador
print("\n📌 Consulta 2: Cantidad de partidos ganados por jugador")
pipeline = [
    {"$group": {"_id": "$Winner", "ganados": {"$sum": 1}}},
    {"$sort": {"ganados": -1}},
    {"$limit": 5}
]
for doc in collection.aggregate(pipeline):
    print(f"{doc['_id']}: {doc['ganados']} victorias")

