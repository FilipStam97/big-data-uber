import argparse
from pathlib import Path

import pandas as pd

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


def load_data(data_path: str) -> pd.DataFrame:
    path = Path(data_path)

    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    print(f"Loading {path}...")
    df = pd.read_parquet(path)

    print(f"Loaded {len(df):,} records.")
    return df

def load_zone_lookup(path: str) -> pd.DataFrame:
    zones = pd.read_csv(path)

    zones = zones.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        }
    )

    return zones

def valid_hour(value: str) -> int:
    hour = int(value)

    if not 0 <= hour <= 23:
        raise argparse.ArgumentTypeError(
            "hour must be between 0 and 23"
        )

    return hour


def enrich_with_zones(
    df: pd.DataFrame,
    zones: pd.DataFrame,
) -> pd.DataFrame:
    pickup_zones = zones.rename(
        columns={
            "location_id": "PULocationID",
            "borough": "pickup_borough",
            "zone": "pickup_zone",
            "service_zone": "pickup_service_zone",
        }
    )

    dropoff_zones = zones.rename(
        columns={
            "location_id": "DOLocationID",
            "borough": "dropoff_borough",
            "zone": "dropoff_zone",
            "service_zone": "dropoff_service_zone",
        }
    )

    df = df.merge(
        pickup_zones,
        on="PULocationID",
        how="left",
    )

    df = df.merge(
        dropoff_zones,
        on="DOLocationID",
        how="left",
    )

    return df

def filter_trips(df: pd.DataFrame, args) -> pd.DataFrame:
    result = df

    if args.start_date:
        result = result[
            result["pickup_datetime"] >= pd.to_datetime(args.start_date)
        ]

    if args.end_date:
        result = result[
            result["pickup_datetime"] <= pd.to_datetime(args.end_date)
        ]

    if args.pickup_zone is not None:
        result = result[
            result["PULocationID"] == args.pickup_zone
        ]

    if args.dropoff_zone is not None:
        result = result[
            result["DOLocationID"] == args.dropoff_zone
        ]

    if args.min_distance is not None:
        result = result[
            result["trip_miles"] >= args.min_distance
        ]

    if args.max_distance is not None:
        result = result[
            result["trip_miles"] <= args.max_distance
        ]

    if args.hour is not None:
        result = result[
            result["pickup_hour"] == args.hour
    ]

    if args.weekday is not None:
        result = result[
            result["pickup_weekday"].str.lower() == args.weekday.lower()
        ]

    return result


def trips_command(df: pd.DataFrame, args):
    result = filter_trips(df, args)

    if args.sort_by:
        result = result.sort_values(
            by=args.sort_by,
            ascending=not args.desc
        )

    print(f"\nMatching trips: {len(result):,}\n")

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

    print(result[columns].head(args.limit).to_string(index=False))


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["pickup_day"] = df["pickup_datetime"].dt.day
    df["pickup_month"] = df["pickup_datetime"].dt.month
    df["pickup_weekday"] = df["pickup_datetime"].dt.day_name()

    return df


def stats_command(df: pd.DataFrame, args):
    stats = (
        df.groupby(args.group_by)[args.attribute]
        .agg(["count", "min", "max", "mean", "std"])
        .sort_values("count", ascending=False)
    )

    print(stats.head(args.limit).to_string())


def build_parser():
    parser = argparse.ArgumentParser(
        description="NYC HVFHV trip data analysis"
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
    trips_parser.add_argument("--desc", action="store_true")
    trips_parser.add_argument("--limit", type=int, default=20)
    trips_parser.add_argument("--hour", type=valid_hour)
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

    df = load_data(args.data)

    zones = load_zone_lookup(args.zones)
    df = enrich_with_zones(df, zones)

    df = add_time_features(df)

    if args.command == "trips":
        trips_command(df, args)

    elif args.command == "stats":
        stats_command(df, args)


if __name__ == "__main__":
    main()