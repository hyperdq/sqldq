import polars as pl
import pytest


@pytest.fixture(scope="session")
def df_users() -> pl.DataFrame:
    return pl.DataFrame({
        "user_id": [1, 2, 3, 4, 4],  # Duplicate user_id 4
        "age": [25, 150, 22, 45, 30],  # Age 150 is outlier
        "email": [
            "user1@example.com",
            "user2@example.com",
            "invalid-email",
            "user4@example.com",
            "user5@example.com",
        ],  # Invalid email for user_id 3
        "score": [85.5, 92.0, 78.3, 88.7, 95.2],
        "is_active": [True, True, False, True, False],
        "category": ["A", "B", "B", "A", "C"],  # User_id 3: False, B violates business rule
    })


@pytest.fixture(scope="session")
def df_orders() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 5],  # user_id 5 missing in users table
        "order_date": ["2021-01-01", "2021-01-02", "2021-01-03"],
        "order_amount": [100, 200, 300],
    })
