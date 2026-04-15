"""
schemas.py — Schémas Avro partagés entre users_service et product_service.
Placer ce fichier dans le même dossier que les autres services.

Installation : pip install fastavro
"""

import io
import fastavro

USER_REQUEST_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "UserRequest",
    "namespace": "com.demo.kafka",
    "fields": [
        {"name": "user_id",    "type": "int"},
        {"name": "request_id", "type": "string"},
        {"name": "timestamp",  "type": "long"},
    ]
})

PRODUCT_RESPONSE_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "ProductResponse",
    "namespace": "com.demo.kafka",
    "fields": [
        {"name": "user_id",    "type": "int"},
        {"name": "request_id", "type": "string"},
        {"name": "products",   "type": {"type": "array", "items": {
            "type": "record",
            "name": "Product",
            "fields": [
                {"name": "id",   "type": "int"},
                {"name": "name", "type": "string"},
            ]
        }}, "default": []},
        {"name": "error", "type": ["null", "string"], "default": None},
    ]
})

DEAD_LETTER_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "DeadLetter",
    "namespace": "com.demo.kafka",
    "fields": [
        {"name": "original_topic", "type": "string"},
        {"name": "raw_payload",    "type": "bytes"},
        {"name": "error_message",  "type": "string"},
        {"name": "retry_count",    "type": "int"},
        {"name": "failed_at",      "type": "long"},
    ]
})


def serialize(record: dict, schema) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def deserialize(data: bytes, schema) -> dict:
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, schema)
