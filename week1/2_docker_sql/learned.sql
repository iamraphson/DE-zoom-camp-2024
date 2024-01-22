-- Some leanred SQL statements

select count(1) from zones;
select * from zones;
SELECT * from yellow_taxi_data_2021_01 LIMIT 100;
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(lpu. "Borough", ' / ', lpu. "Zone") AS pick_up_loc,
	CONCAT(ldo. "Borough", '/  ', ldo. "Zone") AS drop_off_loc
FROM
	yellow_taxi_data_2021_01 t
	JOIN zones lpu ON t. "PULocationID" = lpu. "LocationID"
	JOIN zones ldo ON t. "DOLocationID" = ldo. "LocationID"
LIMIT 100;


SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	CONCAT(lpu. "Borough", ' / ', lpu. "Zone") AS pick_up_loc,
	CONCAT(ldo. "Borough", '/  ', ldo. "Zone") AS drop_off_loc
FROM
	yellow_taxi_data_2021_01 t
	JOIN zones lpu ON t. "PULocationID" = lpu. "LocationID"
	JOIN zones ldo ON t. "DOLocationID" = ldo. "LocationID";


SELECT
	max(total_amount),
	max(passenger_count),
	"DOLocationID",
	CAST(tpep_dropoff_datetime as DATE) as "day",
	count(*) as "count"
FROM
	yellow_taxi_data_2021_01 t GROUP by CAST(tpep_dropoff_datetime as DATE), "DOLocationID"
	order by "day" ASC, "DOLocationID" ASC;
