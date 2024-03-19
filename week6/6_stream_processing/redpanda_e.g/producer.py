import json
import csv
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from ride import Ride
import time

from settings import BOOTSTRAP_SERVERS, RIDE_KAFKA_TOPIC, INPUT_DATA_PATH

class RedpandaJsonProducer(KafkaProducer):
    def __init__(self, props: dict):
        self.producer = KafkaProducer(**props)
        
    @staticmethod
    def read_records(resource_path: str): 
        records = []
        with open(resource_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                records.append(Ride(arr=row))
                
        return records
    
    def publish_rides(self, topic: str, rides: list[Ride]):
        for ride in rides:
            try:
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
                time.sleep(0.5)
                print(f'Record {ride.pu_location_id} successfully produced at offset {record.get().offset}')
            except KafkaTimeoutError as e:
                print(e.__str__())
    
if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda value: json.dumps(value.__dict__, default=str).encode('utf-8')
    }
    
    producer = RedpandaJsonProducer(config)
    rides = producer.read_records(INPUT_DATA_PATH)
    #print(len(rides))
    producer.publish_rides(topic=RIDE_KAFKA_TOPIC, rides=rides)