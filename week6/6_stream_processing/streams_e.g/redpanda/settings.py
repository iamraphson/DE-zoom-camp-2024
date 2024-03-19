import pyspark.sql.types as T

INPUT_DATA_PATH = '../../resources/rides.csv'
BOOTSTRAP_SERVERS = 'localhost:9093'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed.via.redpanda'
PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv.via.redpanda'

RIDE_SCHEMA = T.StructType([
    T.StructField("vendor_id", T.IntegerType()),
    T.StructField('tpep_pickup_datetime', T.TimestampType()),
    T.StructField('tpep_dropoff_datetime', T.TimestampType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.FloatType()),
    T.StructField("payment_type", T.IntegerType()),
    T.StructField("total_amount", T.FloatType()),
])