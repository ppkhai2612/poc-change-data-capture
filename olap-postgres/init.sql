CREATE TABLE IF NOT EXISTS feature_usage_counts (
    feature_name VARCHAR(255) PRIMARY KEY,
    count BIGINT NOT NULL
);