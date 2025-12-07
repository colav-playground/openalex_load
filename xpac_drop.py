from pymongo import MongoClient

# Conexión al servidor MongoDB
client = MongoClient("mongodb://localhost:27017")

# Selección de la base de datos y colección
db = client["openalex"]
collection = db["works"]

# Eliminación de documentos que tengan el campo is_xpack
result = collection.delete_many({"is_xpac": True})

print(f"Documentos eliminados: {result.deleted_count}")
