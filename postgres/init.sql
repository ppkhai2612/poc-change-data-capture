CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    signup_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE feature_usage (
    id SERIAL PRIMARY KEY,
    user_id INT,
    feature_name TEXT,
    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

INSERT INTO users (email) VALUES
('alice@example.com'), ('bob@example.com');

INSERT INTO feature_usage (user_id, feature_name)
VALUES (1, 'dashboard'), (2, 'analytics'), (1, 'exports');