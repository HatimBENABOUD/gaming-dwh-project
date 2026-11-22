import pandas as pd
from sqlalchemy import create_engine
import time


DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "steam_dwh"

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

def ingest_games():
    print("⏳ Ingestion de games.csv...")
    df = pd.read_csv("data/raw/games.csv")
    df.to_sql("bronze_raw_games", con=engine, if_exists="append", index=False)
    print(f"✅ {len(df)} jeux insérés dans 'bronze_raw_games'.")

def ingest_users():
    print("⏳ Ingestion de users.csv...")
    df = pd.read_csv("data/raw/users.csv")
    df.to_sql("bronze_raw_users", con=engine, if_exists="append", index=False)
    print(f"✅ {len(df)} utilisateurs insérés dans 'bronze_raw_users'.")

def ingest_recommendations_chunked():
    print("⏳ Ingestion de recommendations.csv (par paquets de 100 000 lignes)...")
    start_time = time.time()
    chunk_size = 100000
    total_rows = 0

    for chunk in pd.read_csv("data/raw/recommendations.csv", chunksize=chunk_size):
        chunk = chunk.rename(columns={"date": "date_posted"})
        chunk.to_sql("bronze_raw_recommendations", con=engine, if_exists="append", index=False)
        total_rows += len(chunk)
        print(f"   -> {total_rows} lignes chargées...")

    print(f"✅ {total_rows} recommandations insérées en {round(time.time() - start_time, 2)} secondes.")

if __name__ == "__main__":
    print("🚀 Début de l'ingestion dans la Couche Bronze...")
    ingest_games()
    ingest_users()
    ingest_recommendations_chunked()
    print("🎉 Ingestion Bronze terminée avec succès !")