import os
import csv
from time import sleep

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import RideRecordKey, ride_record_key_to_dict
from ride_record_value import RideRecordValue, ride_record_to_dict


from settings import RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVERS, INPUT_DATA_PATH, AVRO_RIDE_KAFKA_TOPIC

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

class AvroProducer:
    def __init__(self, props: dict) -> None:
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_client = SchemaRegistryClient({
            'url': props['schema_registry.url']
        })
    
        self.key_serializer = AvroSerializer(schema_registry_client, key_schema_str, ride_record_key_to_dict)
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, ride_record_to_dict)
        
        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)
        
    @staticmethod
    def read_records(resource_path: str):
        ride_records, ride_keys = [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                ride_records.append(RideRecordValue(arr=[row[0], row[3], row[4], row[9], row[16]]))
                ride_keys.append(RideRecordKey(vendor_id=int(row[0])))
        return zip(ride_keys, ride_records)
        
    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str
    
    def publish(self, topic: str, records: list[RideRecordKey, RideRecordValue]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.produce(
                    topic=topic,
                    key=self.key_serializer(key, SerializationContext(topic=topic, field=MessageField.KEY)),
                    value=self.value_serializer(value, SerializationContext(topic=topic, field=MessageField.VALUE)),
                    on_delivery=delivery_report
                )
                self.producer.flush()
                sleep(0.5)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")
        sleep(1)
    
    
if __name__ == "__main__":
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,
        'schema.key': RIDE_KEY_SCHEMA_PATH,
        'schema.value': RIDE_VALUE_SCHEMA_PATH
    }
    producer = AvroProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish(topic=AVRO_RIDE_KAFKA_TOPIC, records=ride_records)
