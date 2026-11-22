CREATE DATABASE IF NOT EXISTS steam_dwh;
USE steam_dwh;

-- Table brute pour les jeux
CREATE TABLE IF NOT EXISTS bronze_raw_games (
    app_id INT,
    title VARCHAR(255),
    date_release VARCHAR(50),
    win VARCHAR(10),
    mac VARCHAR(10),
    linux VARCHAR(10),
    rating VARCHAR(50),
    positive_ratio INT,
    user_reviews INT,
    price_final DECIMAL(10,2),
    price_original DECIMAL(10,2),
    discount DECIMAL(5,2),
    steam_deck VARCHAR(10),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table brute pour les utilisateurs
CREATE TABLE IF NOT EXISTS bronze_raw_users (
    user_id INT,
    products INT,
    reviews INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table brute pour les recommandations (Télémétrie & Avis)
CREATE TABLE IF NOT EXISTS bronze_raw_recommendations (
    app_id INT,
    helpful INT,
    funny INT,
    date_posted VARCHAR(50),
    is_recommended VARCHAR(10),
    hours DECIMAL(10,1),
    user_id INT,
    review_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);