import os

# Set environment variable BEFORE importing other modules to mock the postgres connection with SQLite
os.environ["DATABASE_URL"] = "sqlite:///:memory:"

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db
from main import app

# Database configuration for tests (in-memory SQLite)
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency override
def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

@pytest.fixture(autouse=True)
def test_db():
    # Setup test database
    Base.metadata.create_all(bind=engine)
    yield
    # Teardown test database
    Base.metadata.drop_all(bind=engine)

client = TestClient(app)

def test_create_expense():
    expense_payload = {
        "expense_name": "Office Stationery Purchase",
        "seller_name": "Stationery World",
        "gst_amount": 450.00,
        "total_amount": 2850.00,
        "purchase_date": "2026-04-05",
        "receipt_copy": "receipt_20260405_001.pdf",
        "priority": "Medium",
        "paid_by": "Rahul Sharma",
        "mode_of_payment": "UPI",
        "for_property": ["Clover Villa", "Kitchen"],
        "submitted_by": "Anonymous",
        "submitted_at": "2026-04-08T18:38:00+05:30"
    }

    response = client.post("/expense", json=expense_payload)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    assert data["message"] == "Expense record ingested successfully."
    assert "id" in data["data"]

def test_create_income():
    income_payload = {
        "for_property": ["Clover Villa"],
        "name": "Amit Kumar Sharma",
        "receipt_date": "08-04-2026 18:35",
        "booking_number": "BK20260408-0456",
        "payment_type": "UPI",
        "room_amount": 4500.00,
        "food_amount": 850.00,
        "payment_screenshot": "payment_screenshot_20260408_1835.jpg",
        "receipt_by": "Rahul Patel",
        "submitted_by": "Anonymous",
        "submitted_at": "2026-04-08T18:50:00+05:30"
    }

    response = client.post("/income", json=income_payload)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    assert data["message"] == "Income record ingested successfully."
    assert "id" in data["data"]

def test_missing_fields_expense():
    expense_payload = {
        "expense_name": "Missing Required Fields Test",
    }
    response = client.post("/expense", json=expense_payload)
    assert response.status_code == 422 # Pydantic validation error
