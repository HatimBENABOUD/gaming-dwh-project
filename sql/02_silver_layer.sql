USE steam_dwh;

-- ==========================================
-- 1. Nettoyage de la table des jeux (silver_stg_games)
-- ==========================================
DROP TABLE IF EXISTS silver_stg_games;

CREATE TABLE silver_stg_games AS
SELECT
    app_id,
    TRIM(title) AS game_title,
    STR_TO_DATE(date_release, '%Y-%m-%d') AS release_date,
    CASE WHEN LOWER(win) IN ('true', '1') THEN 1 ELSE 0 END AS is_windows,
    CASE WHEN LOWER(mac) IN ('true', '1') THEN 1 ELSE 0 END AS is_mac,
    CASE WHEN LOWER(linux) IN ('true', '1') THEN 1 ELSE 0 END AS is_linux,
    CASE WHEN LOWER(steam_deck) IN ('true', '1') THEN 1 ELSE 0 END AS is_steam_deck,
    COALESCE(NULLIF(TRIM(rating), ''), 'Unrated') AS rating,
    COALESCE(positive_ratio, 0) AS positive_ratio,
    COALESCE(user_reviews, 0) AS user_reviews_count,
    CAST(COALESCE(price_final, 0) AS DECIMAL(10,2)) AS price_final,
    CAST(COALESCE(price_original, 0) AS DECIMAL(10,2)) AS price_original,
    CAST(COALESCE(discount, 0) AS DECIMAL(5,2)) AS discount_pct,
    CASE WHEN COALESCE(price_final, 0) = 0 THEN 1 ELSE 0 END AS is_free_game
FROM bronze_raw_games
WHERE app_id IS NOT NULL;

-- Clé primaire et index de performance
ALTER TABLE silver_stg_games ADD PRIMARY KEY (app_id);


-- ==========================================
-- 2. Nettoyage de la table des utilisateurs (silver_stg_users)
-- ==========================================
DROP TABLE IF EXISTS silver_stg_users;

CREATE TABLE silver_stg_users AS
SELECT
    user_id,
    COALESCE(products, 0) AS total_products_owned,
    COALESCE(reviews, 0) AS total_reviews_written
FROM bronze_raw_users
WHERE user_id IS NOT NULL;

-- Clé primaire
ALTER TABLE silver_stg_users ADD PRIMARY KEY (user_id);


-- ==========================================
-- 3. Nettoyage des recommandations (silver_stg_recommendations - 4M+ lignes)
-- ==========================================
DROP TABLE IF EXISTS silver_stg_recommendations;

CREATE TABLE silver_stg_recommendations AS
SELECT
    review_id,
    app_id,
    user_id,
    COALESCE(helpful, 0) AS helpful_votes,
    COALESCE(funny, 0) AS funny_votes,
    STR_TO_DATE(date_posted, '%Y-%m-%d') AS date_posted,
    CASE WHEN LOWER(is_recommended) IN ('true', '1') THEN 1 ELSE 0 END AS is_recommended,
    CAST(GREATEST(COALESCE(hours, 0), 0) AS DECIMAL(10,1)) AS hours_played
FROM bronze_raw_recommendations
WHERE review_id IS NOT NULL
  AND app_id IS NOT NULL
  AND user_id IS NOT NULL;

-- Clé primaire et index pour accélérer la jointure lors de la Phase Gold
ALTER TABLE silver_stg_recommendations ADD PRIMARY KEY (review_id);
CREATE INDEX idx_silver_rec_app ON silver_stg_recommendations(app_id);
CREATE INDEX idx_silver_rec_user ON silver_stg_recommendations(user_id);