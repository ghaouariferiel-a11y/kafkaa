from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time
import uuid
import logging

from schemas import (
    serialize, deserialize,
    USER_REQUEST_SCHEMA, PRODUCT_RESPONSE_SCHEMA,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
MAX_RETRIES  = 3
RETRY_BACKOFF = 1.0  

# Métriques Prometheus
MSG_SENT = Counter(
    "kafka_messages_sent_total",
    "Messages envoyés sur Kafka", ["topic"]
)
MSG_FAILED = Counter(
    "kafka_messages_failed_total",
    "Messages définitivement en erreur après retries", ["topic"]
)
REQ_DURATION = Histogram(
    "request_processing_seconds",
    "Durée de traitement d'une requête",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

#  Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v,  # bytes Avro bruts
    retries=0,
)

consumer = KafkaConsumer(
    'product_responses',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: v,  # bytes Avro bruts
    auto_offset_reset='latest',
    group_id='users-group',
    consumer_timeout_ms=8000,
)

# DB
users_db = {
    1: {"id": 1, "name": "Alice"},
    2: {"id": 2, "name": "Bob"}
}


def send_with_retry(topic: str, payload: bytes, request_id: str) -> bool:
    """Envoie un message Kafka avec jusqu'à MAX_RETRIES tentatives (backoff exponentiel)."""
    backoff = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            future = producer.send(topic, payload)
            producer.flush(timeout=5)
            future.get(timeout=5)
            MSG_SENT.labels(topic=topic).inc()
            log.info(" Envoyé (tentative %d/3) request_id=%s", attempt, request_id)
            return True
        except (KafkaError, Exception) as exc:
            log.warning("  Échec Kafka tentative %d/%d : %s — retry dans %.1fs", attempt, MAX_RETRIES, exc, backoff)
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2
    MSG_FAILED.labels(topic=topic).inc()
    log.error(" Message perdu après %d tentatives — request_id=%s", MAX_RETRIES, request_id)
    return False


@app.get("/users/{user_id}")
async def get_user(user_id: int):
    start = time.perf_counter()

    user = users_db.get(user_id)
    if not user:
        return {"error": "User not found"}

    request_id = str(uuid.uuid4())

    # Sérialisation Avro (validation du schéma à l'émission)
    payload = serialize(
        {
            "user_id":    user_id,
            "request_id": request_id,
            "timestamp":  int(time.time() * 1000),
        },
        USER_REQUEST_SCHEMA,
    )

    # Envoi avec retry
    if not send_with_retry('user_requests', payload, request_id):
        REQ_DURATION.observe(time.perf_counter() - start)
        raise HTTPException(status_code=503, detail="Kafka indisponible après retries")

    # Attente de la réponse
    result = {**user, "products": []}
    try:
        for message in consumer:
            data = deserialize(message.value, PRODUCT_RESPONSE_SCHEMA)
            if data.get("request_id") == request_id:
                if data.get("error"):
                    log.error("Erreur reçue de product_service : %s", data["error"])
                else:
                    result["products"] = data.get("products", [])
                break
    except StopIteration:
        log.warning("Timeout en attente réponse pour request_id=%s", request_id)

    REQ_DURATION.observe(time.perf_counter() - start)
    return result


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Endpoint scrapé par Prometheus — http://localhost:8000/metrics"""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
