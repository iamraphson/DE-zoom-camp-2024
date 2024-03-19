from kafka import KafkaConsumer
import json

from ride import Ride
from settings import BOOTSTRAP_SERVERS, RIDE_KAFKA_TOPIC, INPUT_DATA_PATH

class RedpandaJsonConsumer:
    def __init__(self, props: dict) -> None:
        self.consumer = KafkaConsumer(**props)
    
    def consume(self, topics: list[str]):
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        
        while True:
            try: 
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                
                for _, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
                        
            except KeyboardInterrupt:
                break;
        
        self.consumer.close()
        
if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'group_id': 'redpanda.ride.consumer',
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda value: json.loads(value.decode('utf-8'), object_hook=lambda data: Ride.from_dict(d=data))
    }
    
    consumer = RedpandaJsonConsumer(config)
    consumer.consume(topics=[RIDE_KAFKA_TOPIC])