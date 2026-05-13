"""Create Kafka topics required by the pipeline.

Run after `docker compose up -d kafka`.
"""

from __future__ import annotations

import os
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "6"))

TOPICS = [
    (os.getenv("KAFKA_TOPIC_RATINGS", "movielens.ratings.raw"), NUM_PARTITIONS),
    (os.getenv("KAFKA_TOPIC_TRENDING", "movielens.trending.5m"), NUM_PARTITIONS),
    (os.getenv("KAFKA_TOPIC_ENRICHED", "movielens.events.enriched"), NUM_PARTITIONS),
]


def main() -> int:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="movielens-bootstrap")
    new_topics = [
        NewTopic(name=name, num_partitions=parts, replication_factor=1)
        for name, parts in TOPICS
    ]
    for topic in new_topics:
        try:
            admin.create_topics([topic])
            print(f"created  {topic.name:32s}  partitions={topic.num_partitions}")
        except TopicAlreadyExistsError:
            print(f"exists   {topic.name}")
    admin.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
