import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SORTABLE_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tips",
    "driver_pay",
]

STAT_ATTRIBUTES = [
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tips",
    "driver_pay",
    "tolls",
    "sales_tax",
]

GROUP_COLUMNS = [
    "PULocationID",
    "DOLocationID",
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "hvfhs_license_num",
    "pickup_hour",
    "pickup_day",
    "pickup_month",
    "pickup_weekday",
]

DISPLAY_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tips",
    "driver_pay",
]


def valid_hour(value: str) -> int:
    hour = int(value)

    if not 0 <= hour <= 23:
        raise argparse.ArgumentTypeError(
            "hour must be between 0 and 23"
        )

    return hour


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NYC HVFHV Trip Analysis")
        .getOrCreate()
    )


def load_data(spark: SparkSession, data_path: str):
    print(f"Loading {data_path}...")
    return spark.read.parquet(data_path)


def load_zone_lookup(spark: SparkSession, path: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def enrich_with_zones(df, zones):
    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    df = df.join(
        pickup_zones,
        on="PULocationID",
        how="left",
    )

    df = df.join(
        dropoff_zones,
        on="DOLocationID",
        how="left",
    )

    return df


def add_time_features(df):
    return (
        df
        .withColumn(
            "pickup_hour",
            F.hour("pickup_datetime"),
        )
        .withColumn(
            "pickup_day",
            F.dayofmonth("pickup_datetime"),
        )
        .withColumn(
            "pickup_month",
            F.month("pickup_datetime"),
        )
        .withColumn(
            "pickup_weekday",
            F.date_format("pickup_datetime", "EEEE"),
        )
    )


def filter_trips(df, args):
    result = df

    if args.start_date:
        result = result.filter(
            F.col("pickup_datetime")
            >= F.to_timestamp(F.lit(args.start_date))
        )

    if args.end_date:
        if len(args.end_date) == 10:
            end = F.to_timestamp(F.lit(args.end_date)) + F.expr("INTERVAL 1 DAY")
            result = result.filter(F.col("pickup_datetime") < end)
        else:
            end = F.to_timestamp(F.lit(args.end_date))
            result = result.filter(F.col("pickup_datetime") <= end)

    if args.pickup_zone is not None:
        result = result.filter(
            F.col("PULocationID") == args.pickup_zone
        )

    if args.dropoff_zone is not None:
        result = result.filter(
            F.col("DOLocationID") == args.dropoff_zone
        )

    if args.min_distance is not None:
        result = result.filter(
            F.col("trip_miles") >= args.min_distance
        )

    if args.max_distance is not None:
        result = result.filter(
            F.col("trip_miles") <= args.max_distance
        )

    if args.hour is not None:
        result = result.filter(
            F.col("pickup_hour") == args.hour
        )

    if args.weekday is not None:
        result = result.filter(
            F.lower(F.col("pickup_weekday"))
            == args.weekday.lower()
        )

    return result


def trips_command(df, args):
    result = filter_trips(df, args)

    if args.sort_by:
        if args.desc:
            result = result.orderBy(
                F.col(args.sort_by).desc()
            )
        else:
            result = result.orderBy(
                F.col(args.sort_by).asc()
            )

    print(f"\nMatching trips: {result.count():,}\n")

    columns = [
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "tips",
        "driver_pay",
    ]

    result.select(*args.columns).show(args.limit, truncate=False)


def stats_command(df, args):
    stats = (
        df.groupBy(args.group_by)
        .agg(
            F.count(args.attribute).alias("count"),
            F.min(args.attribute).alias("min"),
            F.max(args.attribute).alias("max"),
            F.mean(args.attribute).alias("mean"),
            F.stddev(args.attribute).alias("std"),
            F.sum(args.attribute).alias("sum"),
            F.expr(
                f"percentile_approx({args.attribute}, 0.5)"
            ).alias("median"),
        )
        .orderBy(F.desc("count"))
    )

    stats.show(
        args.limit,
        truncate=False,
    )


def build_parser():
    parser = argparse.ArgumentParser(
        description="NYC HVFHV trip data analysis - Spark"
    )

    parser.add_argument(
        "--data",
        required=True,
        help="Path to the Parquet dataset",
    )

    parser.add_argument(
        "--zones",
        default="data/lookup/taxi_zone_lookup.csv",
        help="Path to taxi zone lookup CSV",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    trips_parser = subparsers.add_parser(
        "trips",
        help="Filter and display trips",
    )

    trips_parser.add_argument(
        "--columns",
        nargs="+",
        choices=DISPLAY_COLUMNS,
        default=[
            "pickup_datetime",
            "PULocationID",
            "DOLocationID",
            "pickup_borough",
            "pickup_zone",
            "trip_miles",
            "trip_time",
            "base_passenger_fare",
            "tips",
            "driver_pay",
            ],
    )

    trips_parser.add_argument("--start-date")
    trips_parser.add_argument("--end-date")
    trips_parser.add_argument("--pickup-zone", type=int)
    trips_parser.add_argument("--dropoff-zone", type=int)
    trips_parser.add_argument("--min-distance", type=float)
    trips_parser.add_argument("--max-distance", type=float)

    trips_parser.add_argument(
        "--sort-by",
        choices=SORTABLE_COLUMNS,
    )

    trips_parser.add_argument(
        "--desc",
        action="store_true",
    )

    trips_parser.add_argument(
        "--limit",
        type=int,
        default=20,
    )

    trips_parser.add_argument(
        "--hour",
        type=valid_hour,
    )

    trips_parser.add_argument("--weekday")

    stats_parser = subparsers.add_parser(
        "stats",
        help="Calculate grouped statistics",
    )

    stats_parser.add_argument(
        "--attribute",
        required=True,
        choices=STAT_ATTRIBUTES,
    )

    stats_parser.add_argument(
        "--group-by",
        required=True,
        choices=GROUP_COLUMNS,
    )

    stats_parser.add_argument(
        "--limit",
        type=int,
        default=20,
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    spark = create_spark()

    start_time = time.perf_counter()

    try:
        df = load_data(
            spark,
            args.data,
        )

        zones = load_zone_lookup(
            spark,
            args.zones,
        )

        df = enrich_with_zones(
            df,
            zones,
        )

        df = add_time_features(df)

        if args.command == "trips":
            trips_command(df, args)

        elif args.command == "stats":
            stats_command(df, args)

        elapsed = time.perf_counter() - start_time
        print(f"\nExecution time: {elapsed:.3f} seconds")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()