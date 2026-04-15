from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
import time
import logging

from schemas import (
    serialize, deserialize,
    USER_REQUEST_SCHEMA, PRODUCT_RESPONSE_SCHEMA, DEAD_LETTER_SCHEMA,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
MAX_RETRIES   = 3
RETRY_BACKOFF = 1.0

# Métriques Prometheus
MSG_CONSUMED = Counter(
    "kafka_consumed_total",
    "Messages consommés", ["topic"]
)
PROCESSING_ERRORS = Counter(
    "kafka_processing_errors_total",
    "Erreurs de traitement", ["reason"]
)
DEAD_LETTER_SENT = Counter(
    "kafka_dead_letter_total",
    "Messages envoyés en dead letter queue"
)

# Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v,  # bytes Avro bruts
    retries=0,
)

consumer = KafkaConsumer(
    'user_requests',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: v, 
    auto_offset_reset='latest',
    group_id='products-group'
)

#  DB
products_db = {
    1: [{"id": 1, "name": "Book"}, {"id": 2, "name": "Pen"}],
    2: [{"id": 3, "name": "Laptop"}]
}


def send_with_retry(topic: str, payload: bytes, label: str) -> bool:
    """Envoie un message Kafka avec jusqu'à MAX_RETRIES tentatives (backoff exponentiel)."""
    backoff = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            future = producer.send(topic, payload)
            producer.flush(timeout=5)
            future.get(timeout=5)
            log.info("Envoyé sur %s (tentative %d/3) — %s", topic, attempt, label)
            return True
        except (KafkaError, Exception) as exc:
            log.warning("  Échec %s tentative %d/%d : %s — retry dans %.1fs", topic, attempt, MAX_RETRIES, exc, backoff)
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2
    return False


def send_dead_letter(original_payload: bytes, error_msg: str, retry_count: int):
    """Envoie un message non traitable sur le Dead Letter Queue."""
    try:
        dl_payload = serialize(
            {
                "original_topic": "user_requests",
                "raw_payload":    original_payload,
                "error_message":  error_msg,
                "retry_count":    retry_count,
                "failed_at":      int(time.time() * 1000),
            },
            DEAD_LETTER_SCHEMA,
        )
        producer.send('dead_letter_queue', dl_payload)
        producer.flush(timeout=5)
        DEAD_LETTER_SENT.inc()
        log.error("  Dead letter envoyé — %s", error_msg)
    except Exception as exc:
        log.critical("Impossible d'écrire dans dead_letter_queue : %s", exc)


def consume_requests():
    print("📥 Products Service listening...")
    for message in consumer:
        raw = message.value
        MSG_CONSUMED.labels(topic='user_requests').inc()

        # Désérialisation Avro — validation stricte du schéma
        try:
            request = deserialize(raw, USER_REQUEST_SCHEMA)
        except Exception as exc:
            PROCESSING_ERRORS.labels(reason="deserialization").inc()
            log.error(" Désérialisation Avro impossible : %s", exc)
            send_dead_letter(raw, f"Deserialization error: {exc}", 0)
            continue

        user_id    = request["user_id"]
        request_id = request["request_id"]
        log.info(" user_id=%d request_id=%s", user_id, request_id)

        # Traitement métier
        try:
            products = products_db.get(user_id, [])
            response_payload = serialize(
                {
                    "user_id":    user_id,
                    "request_id": request_id,
                    "products":   products,
                    "error":      None,
                },
                PRODUCT_RESPONSE_SCHEMA,
            )
        except Exception as exc:
            PROCESSING_ERRORS.labels(reason="business_logic").inc()
            log.error(" Erreur métier user_id=%d : %s", user_id, exc)
            response_payload = serialize(
                {
                    "user_id":    user_id,
                    "request_id": request_id,
                    "products":   [],
                    "error":      str(exc),
                },
                PRODUCT_RESPONSE_SCHEMA,
            )

        # Envoi avec retry
        success = send_with_retry('product_responses', response_payload, request_id)
        if not success:
            PROCESSING_ERRORS.labels(reason="send_failed").inc()
            send_dead_letter(raw, "Failed to send product_responses after retries", MAX_RETRIES)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Endpoint scrapé par Prometheus — http://localhost:8001/metrics"""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    consume_requests()
