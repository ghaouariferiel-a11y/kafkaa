"""
dead_letter_worker.py — Rejoue les messages du Dead Letter Queue.

Démarrage :
    py -3.12 dead_letter_worker.py
"""

import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from schemas import deserialize, DEAD_LETTER_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = 'localhost:9092'
MAX_REPLAY_RETRIES = 2

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: v,
)

consumer = KafkaConsumer(
    'dead_letter_queue',
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: v,
    auto_offset_reset='earliest',
    group_id='dead-letter-replayer',
)


def replay(original_topic: str, raw_payload: bytes, error_msg: str, retry_count: int):
    log.info(" Rejeu sur '%s' | erreur_origine='%s'", original_topic, error_msg)
    backoff = 2.0
    for attempt in range(1, MAX_REPLAY_RETRIES + 1):
        try:
            future = producer.send(original_topic, raw_payload)
            producer.flush(timeout=5)
            future.get(timeout=5)
            log.info(" Rejeu réussi (tentative %d/%d)", attempt, MAX_REPLAY_RETRIES)
            return
        except (KafkaError, Exception) as exc:
            log.warning("⚠️  Rejeu échoué tentative %d/%d : %s", attempt, MAX_REPLAY_RETRIES, exc)
            if attempt < MAX_REPLAY_RETRIES:
                time.sleep(backoff)
                backoff *= 2
    log.error("Rejeu impossible — intervention manuelle requise pour topic='%s'", original_topic)


if __name__ == "__main__":
    log.info("  Dead Letter Worker démarré — écoute sur 'dead_letter_queue'")
    for msg in consumer:
        try:
            dl = deserialize(msg.value, DEAD_LETTER_SCHEMA)
        except Exception as exc:
            log.error("Impossible de décoder le message dead letter : %s", exc)
            continue
        replay(
            original_topic=dl["original_topic"],
            raw_payload=dl["raw_payload"],
            error_msg=dl["error_message"],
            retry_count=dl["retry_count"],
        )
