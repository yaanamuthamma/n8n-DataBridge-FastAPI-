# n8n-DataBridge-FastAPI

A FastAPI-based backend service that acts as a data bridge, providing API endpoints to interact with various databases including MongoDB.

## Features

- Connect to MongoDB instances (with or without authentication)
- List collections from a MongoDB database
- Built with FastAPI for high performance async API handling
- Integrated API logging

## Requirements

- Python 3.8+
- FastAPI
- Pydantic
- PyMongo

## Installation

```bash
pip install fastapi pymongo pydantic uvicorn
```

## Running the Server

```bash
uvicorn main:app --reload
```

## API Endpoints

### POST `/api/mongo/list-collections`

Lists all collections in a given MongoDB database.

**Request Body:**

```json
{
  "host": "localhost",
  "port": 27017,
  "database": "mydb",
  "user": "optional_user",
  "password": "optional_password",
  "authDb": "admin"
}
```

**Response:**

```json
{
  "collections": ["collection1", "collection2"]
}
```

## Project Structure

```
.
├── main.py                  # FastAPI app entry point
├── mongo_helper_view.py     # MongoDB API routes
└── README.md
```
