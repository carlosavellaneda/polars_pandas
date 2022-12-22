# Polars and Pandas benchmark

This repo is a really simple and small benchmark between some common and simple operations when processing data in a machine learning project.

## The problem

Sometimes the preprocessing of the data can be problematic for a machine learning project because (i) it is computationally intensive or (ii) the dataset is too big to work with. `pandas` is a great library for data exploration and processing, but it has some drawbacks; it runs sequentially, and it is hard to make it work on parallel due to python's GIL problem. There are some alternatives (e.g. `dask`) but, in my experience, sometimes it is not worth it to rewrite a script to gain speed in computations if the data fits in memory.

`polars` is a DataFrame library written in rust but with an API available in python. It is built upon the safe Arrow2 implementation, which enables efficient resource use and processing performance. It is written for parallel execution, which makes it a good solution for data wrangling, data pipelines, etc.

This small benchmark address the following simple calculations:

* Load a parquet file containing a dataset of more than 51.5 million rows, and 5 columns.
* Create new boolean columns that are true if the lookup value is in a specific column and false otherwise.
* Group the dataset by a specific column and retrieve the sum, the first and the last values of each column in a list of column names.
* Print the head of the resulting aggregated dataframe

## The benchmark

The files `src/pandas_aggregation.py`, `src/polars_aggregation.py`, and `src/main.rs` contain the same process in pandas, in polars and in polars but using rust, respectively. Each script prints the time spent in reading and processing the data.

### How to run the scripts

In the terminal, run the following command:

```
poetry shell
python src/pandas_aggregation.py
python src/polars_aggregation.py
cargo run --release
```

As a side note, Rust's compile process might take a while, but once compiled, the execution process is really fast.

## The results

Running this process locally on laptop retrieves the following results:

* Pandas: 62.57 seconds.
* Polars: 11.621 seconds.
* Polars + rust: 9.8 seconds.

As a conclusion, using polars is 5-6 times faster than pandas.
