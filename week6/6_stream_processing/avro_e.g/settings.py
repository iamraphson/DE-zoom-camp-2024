INPUT_DATA_PATH = '../resources/rides.csv'

RIDE_KEY_SCHEMA_PATH = './schemas/taxi_ride_key.avsc'
RIDE_VALUE_SCHEMA_PATH = './schemas/taxi_ride_value.avsc'

SCHEMA_REGISTRY_URL = 'http://localhost:8081'
BOOTSTRAP_SERVERS = 'localhost:9092'
AVRO_RIDE_KAFKA_TOPIC='data-platform.avro.rides'