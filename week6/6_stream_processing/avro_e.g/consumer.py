import os

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import dict_to_ride_record_key
from ride_record_value import dict_to_ride_record
from settings import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, \
    RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, AVRO_RIDE_KAFKA_TOPIC


class RideAvroConsumer:
    def __init__(self, props: dict):

        # Schema Registry and Serializer-Deserializer Configurations
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_client = SchemaRegistryClient({
            'url': props['schema_registry.url']
        })
        self.key_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            schema_str=key_schema_str,
            from_dict=dict_to_ride_record_key
        )
        self.value_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            schema_str=value_schema_str,
            from_dict=dict_to_ride_record
        )

        consumer_props = {
            'bootstrap.servers': props['bootstrap.servers'],
            'group.id': 'taxirides.avro.consumer',
            'auto.offset.reset': "earliest"
            }
        self.consumer = Consumer(consumer_props)

    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    def consume_from_kafka(self, topics: list[str]):
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                
                key = self.key_deserializer(
                    msg.key(), 
                    SerializationContext(msg.topic(), MessageField.KEY)
                )
                record = self.value_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
                if record is not None:
                    print(f"{key}, {record}")
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,
        'schema.key': RIDE_KEY_SCHEMA_PATH,
        'schema.value': RIDE_VALUE_SCHEMA_PATH,
    }
    avro_consumer = RideAvroConsumer(props=config)
    avro_consumer.consume_from_kafka(topics=[AVRO_RIDE_KAFKA_TOPIC])