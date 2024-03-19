class RideRecordValue:

    def __init__(self, arr: list[str]):
        self.vendor_id = int(arr[0])
        self.passenger_count = int(arr[1])
        self.trip_distance = float(arr[2])
        self.payment_type = int(arr[3])
        self.total_amount = float(arr[4])

    @classmethod
    def from_dict(cls, d: dict):
        return cls(arr=[
            d['vendor_id'],
            d['passenger_count'],
            d['trip_distance'],
            d['payment_type'],
            d['total_amount']
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride_record(obj, _):
    if obj is None:
        return None

    return RideRecordValue.from_dict(obj)


def ride_record_to_dict(ride_record: RideRecordValue, _):
    return ride_record.__dict__