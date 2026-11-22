USE steam_dwh;

DROP TABLE IF EXISTS dim_games;
CREATE TABLE dim_games AS
SELECT
    app_id,
    game_title,
    release_date,
    is_windows,
    is_mac,
    is_linux,
    is_steam_deck,
    rating,
    positive_ratio,
    user_reviews_count,
    price_final,
    price_original,
    discount_pct,
    is_free_game
FROM silver_stg_games;

ALTER TABLE dim_games ADD PRIMARY KEY (app_id);

DROP TABLE IF EXISTS dim_users;
CREATE TABLE dim_users AS
SELECT
    user_id,
    total_products_owned,
    total_reviews_written
FROM silver_stg_users;

ALTER TABLE dim_users ADD PRIMARY KEY (user_id);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date AS
SELECT DISTINCT
    date_posted,
    YEAR(date_posted) AS year,
    QUARTER(date_posted) AS quarter,
    MONTH(date_posted) AS month,
    DAYNAME(date_posted) AS month_name,
    DAYOFWEEK(date_posted) AS day_of_week
FROM silver_stg_recommendations
WHERE date_posted IS NOT NULL;

ALTER TABLE dim_date ADD PRIMARY KEY (date_posted);

DROP TABLE IF EXISTS fact_recommendations;
CREATE TABLE fact_recommendations AS
SELECT
    review_id,
    app_id,
    user_id,
    date_posted,
    helpful_votes,
    funny_votes,
    is_recommended,
    hours_played
FROM silver_stg_recommendations;

ALTER TABLE fact_recommendations ADD PRIMARY KEY (review_id);
CREATE INDEX idx_fact_game ON fact_recommendations(app_id);
CREATE INDEX idx_fact_user ON fact_recommendations(user_id);
CREATE INDEX idx_fact_date ON fact_recommendations(date_posted);