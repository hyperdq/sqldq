-- Initialize the database with sample data for sqldq testing

-- Drop and recreate the users table
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id INTEGER,
    age INTEGER,
    email VARCHAR(255),
    score DECIMAL(5, 2),
    is_active BOOLEAN,
    category VARCHAR(10)
);

INSERT INTO
    users (
        user_id,
        age,
        email,
        score,
        is_active,
        category
    )
VALUES (
        1,
        25,
        'user1@example.com',
        85.5,
        TRUE,
        'A'
    ),
    (
        2,
        150,
        'user2@example.com',
        92.0,
        TRUE,
        'B'
    ),
    (
        3,
        22,
        'invalid-email',
        78.3,
        FALSE,
        'B'
    ),
    (
        4,
        45,
        'user4@example.com',
        88.7,
        TRUE,
        'A'
    ),
    (
        4,
        30,
        'user5@example.com',
        95.2,
        FALSE,
        'C'
    );

-- Drop and recreate the orders table
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id INTEGER,
    user_id INTEGER,
    order_date DATE,
    order_amount DECIMAL(10, 2)
);

INSERT INTO
    orders (
        order_id,
        user_id,
        order_date,
        order_amount
    )
VALUES (1, 1, '2021-01-01', 100.00),
    (2, 2, '2021-01-02', 200.00),
    (3, 5, '2021-01-03', 300.00);

-- Grant permissions to admin user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;
