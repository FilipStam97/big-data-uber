### Big data uber

## Commands:
python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --pickup-zone 132 --min-distance 5 --sort-by trip_miles --desc --limit 10

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by PULocationID --limit 10

## -------------------------------------

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute base_passenger_fare --group-by pickup_zone --limit 20

## --------------------------------------

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by pickup_hour --limit 24

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute base_passenger_fare --group-by pickup_weekday --limit 7

Useful for presenation:
python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_time --group-by pickup_borough --limit 10

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --pickup-zone 132 --hour 18 --min-distance 5 --sort-by trip_miles --desc --limit 10

## Spark test
python src\spark_main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by pickup_borough --limit 10

python src\spark_main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --pickup-zone 132 --min-distance 5 --sort-by trip_miles --desc --limit 10


## Check if datanode is registered
docker exec -it namenode hdfs dfsadmin -report


create project directories:

docker exec -it namenode hdfs dfs -mkdir -p /uber/raw
docker exec -it namenode hdfs dfs -mkdir -p /uber/lookup

verify: docker exec -it namenode hdfs dfs -ls /uber