import time
import pandas as pd

FILE = "cases_data.parquet"
NEW_COLS = ["mediation", "cases_automator", "cases_massive"]
COLS_TO_AGG = [
    "mediation", "cases_automator", "cases_massive", "FLAG_INCOMING_GESTION", "FLAG_OUTGOING_GESTION"
]


def read_file(file: str) -> pd.DataFrame:
    """Function that reads a parquet file located in `file` path"""
    return pd.read_parquet(file)


def create_feat_on_regex(dataset: pd.DataFrame, column: str, lookup_value: str) -> pd.Series:
    """
    Function that creates a new boolean column that contains True if the column has the lookup value
    and False otherwise
    """
    return dataset[column].str.contains(lookup_value)


def aggregate_data(dataset: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Function that aggregates a grouped dataframe, retrieving the sum, the first and the last value
    of each column stated in the `columns` list.
    """
    dataset = dataset.groupby("CAS_CASE_ID")[columns].agg(["sum", "first", "last"])
    return dataset


def main() -> None:
    start = time.time()
    dataset = read_file(FILE)
    for column in NEW_COLS:
        dataset[column] = create_feat_on_regex(dataset=dataset, column="CI_EVENT_NAME", lookup_value=column)
    agg_dataset = aggregate_data(dataset=dataset, columns=COLS_TO_AGG)
    print(agg_dataset.head())
    end = time.time()
    print(f"Spent {end - start} seconds in processing the data")
    agg_dataset.reset_index(inplace=True)
    print(agg_dataset[agg_dataset["CAS_CASE_ID"] == 22368047.0])


if __name__ == "__main__":
    main()
