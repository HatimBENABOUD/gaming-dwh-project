from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
    .appName("Steam_Gaming_DataLake_Medallion") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
start_total = time.time()

print("Démarrage du Pipeline Lakehouse avec PySpark...")


print("\n--- [BRONZE LAYER] Ingestion des fichiers bruts CSV ---")

df_games_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/raw/games.csv")
df_games_raw.write.mode("overwrite").parquet("data/lakehouse/bronze/games")
print(f"Bronze Games enregistrés ({df_games_raw.count()} lignes).")

df_users_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/raw/users.csv")
df_users_raw.write.mode("overwrite").parquet("data/lakehouse/bronze/users")
print(f"Bronze Users enregistrés ({df_users_raw.count()} lignes).")

print("Chargement des 40M+ de recommandations...")
df_rec_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/raw/recommendations.csv")
df_rec_raw.write.mode("overwrite").parquet("data/lakehouse/bronze/recommendations")
print("Bronze Recommendations enregistrées.")


print("\n--- [SILVER LAYER] Nettoyage et Normalisation des données ---")

df_games_silver = df_games_raw \
    .filter(F.col("app_id").isNotNull()) \
    .withColumn("game_title", F.trim(F.col("title"))) \
    .withColumn("release_date", F.to_date(F.col("date_release"), "M/d/yyyy")) \
    .withColumn("is_windows", F.when(F.lower(F.col("win")) == "true", 1).otherwise(0)) \
    .withColumn("is_mac", F.when(F.lower(F.col("mac")) == "true", 1).otherwise(0)) \
    .withColumn("is_linux", F.when(F.lower(F.col("linux")) == "true", 1).otherwise(0)) \
    .withColumn("is_steam_deck", F.when(F.lower(F.col("steam_deck")) == "true", 1).otherwise(0)) \
    .withColumn("rating", F.coalesce(F.col("rating"), F.lit("Unrated"))) \
    .withColumn("positive_ratio", F.coalesce(F.col("positive_ratio").cast("int"), F.lit(0))) \
    .withColumn("price_final", F.coalesce(F.col("price_final").cast("double"), F.lit(0.0))) \
    .withColumn("is_free_game", F.when(F.col("price_final") == 0, 1).otherwise(0)) \
    .select("app_id", "game_title", "release_date", "is_windows", "is_mac", "is_linux", 
            "is_steam_deck", "rating", "positive_ratio", "price_final", "is_free_game")

df_games_silver.write.mode("overwrite").parquet("data/lakehouse/silver/games")
print("Silver Games nettoyés.")

df_users_silver = df_users_raw \
    .filter(F.col("user_id").isNotNull()) \
    .withColumn("total_products_owned", F.coalesce(F.col("products").cast("int"), F.lit(0))) \
    .withColumn("total_reviews_written", F.coalesce(F.col("reviews").cast("int"), F.lit(0))) \
    .select("user_id", "total_products_owned", "total_reviews_written")

df_users_silver.write.mode("overwrite").parquet("data/lakehouse/silver/users")
print("Silver Users nettoyés.")

df_rec_silver = df_rec_raw \
    .filter(F.col("review_id").isNotNull() & F.col("app_id").isNotNull() & F.col("user_id").isNotNull()) \
    .withColumn("helpful_votes", F.coalesce(F.col("helpful").cast("int"), F.lit(0))) \
    .withColumn("funny_votes", F.coalesce(F.col("funny").cast("int"), F.lit(0))) \
    .withColumn("date_posted", F.to_date(F.col("date"), "yyyy-MM-dd")) \
    .withColumn("is_recommended", F.when(F.lower(F.col("is_recommended")).isin("true", "1"), 1).otherwise(0)) \
    .withColumn("hours_played", F.greatest(F.coalesce(F.col("hours").cast("double"), F.lit(0.0)), F.lit(0.0))) \
    .select("review_id", "app_id", "user_id", "helpful_votes", "funny_votes", "date_posted", "is_recommended", "hours_played")

df_rec_silver.write.mode("overwrite").parquet("data/lakehouse/silver/recommendations")
print("Silver Recommendations nettoyées (40M+ lignes).")


print("\n--- [GOLD LAYER] Structuration en Schéma en Étoile ---")

dim_games = df_games_silver
dim_games.write.mode("overwrite").parquet("data/lakehouse/gold/dim_games")

dim_users = df_users_silver
dim_users.write.mode("overwrite").parquet("data/lakehouse/gold/dim_users")

dim_date = df_rec_silver.select("date_posted").distinct() \
    .filter(F.col("date_posted").isNotNull()) \
    .withColumn("year", F.year("date_posted")) \
    .withColumn("quarter", F.quarter("date_posted")) \
    .withColumn("month", F.month("date_posted")) \
    .withColumn("month_name", F.date_format("date_posted", "MMMM")) \
    .withColumn("day_of_week", F.dayofweek("date_posted"))

dim_date.write.mode("overwrite").parquet("data/lakehouse/gold/dim_date")

fact_recommendations = df_rec_silver
fact_recommendations.write.mode("overwrite").parquet("data/lakehouse/gold/fact_recommendations")

print("Couche Gold construite avec succès (Fact & Dimensions).")
print(f"Pipeline terminé avec succès en {round(time.time() - start_total, 2)} secondes !")

spark.stop()