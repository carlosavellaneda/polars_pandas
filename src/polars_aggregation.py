import time
import polars as pl

FILE = "cases_data.parquet"
NEW_COLS = ["mediation", "cases_automator", "cases_massive"]
COLS_TO_AGG = [
    "mediation", "cases_automator", "cases_massive", "incoming", "outgoing"
]


def read_file(file: str) -> pl.DataFrame:
    """Function that reads a parquet file located in `file` path"""
    return pl.read_parquet(file)


def create_feat_on_regex(dataset: pl.DataFrame, column: str, lookup_value: str) -> pl.DataFrame:
    """
    Function that creates a new boolean column that contains True if the column has the lookup value
    and False otherwise
    """
    return dataset.with_column(pl.col(column).str.contains(lookup_value).alias(lookup_value))


def aggregate_data(dataset: pl.DataFrame, columns: list) -> pl.DataFrame:
    """
    Function that creates a polars expression to aggregate a dataframe, and then executes the
    aggregation process. It receives a list of columns and for each column it retrieves the sum,
    the first and the last value of the column.
    """
    aggregation_list = [
        pl.col(column).sum().alias(column + "_sum") for column in columns
    ] + [
        pl.col(column).first().alias(column + "_first") for column in columns
    ] + [
        pl.col(column).last().alias(column + "_last") for column in columns
    ]
    return dataset.select(columns + ["case_id"]).groupby("case_id").agg(aggregation_list)


def main() -> None:
    start = time.time()
    print("reading the data")
    dataset = read_file(FILE)
    for column in NEW_COLS:
        dataset = create_feat_on_regex(dataset=dataset, column="event_name", lookup_value=column)
    agg_dataset = aggregate_data(dataset=dataset, columns=COLS_TO_AGG)
    print(agg_dataset.head())
    end = time.time()
    print(f"Spent {end - start} seconds in processing the data")
    print(agg_dataset.filter(pl.col("case_id") == 22368047.0))


if __name__ == "__main__":
    main()
