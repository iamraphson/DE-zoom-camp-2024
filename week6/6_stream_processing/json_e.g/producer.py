import csv
import json
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, RIDE_KAFKA_TOPIC
import time

class JsonProducer(KafkaProducer):
    def __init__(self, props: dict):
       self.producer = KafkaProducer(**props)
    
    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as file:
            reader = csv.reader(file)
            header = next(reader) #skip the header row
            for row in reader:
                records.append(Ride(arr=row))
        
        return records
    
    def publish_rides(self, topic: str, rides: list[Ride]):
        for ride in rides:
            try: 
                record = self.producer.send(topic=topic, key=ride.pu_location_id,value=ride)
                print(f'Record {ride.pu_location_id} successfully produced at offset {record.get().offset}')
                time.sleep(0.5)
            except KafkaTimeoutError as e:
                print(e.__str__())
        
        
if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish_rides(topic=RIDE_KAFKA_TOPIC, rides=rides)