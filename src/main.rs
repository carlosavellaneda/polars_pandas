use anyhow::Result;
use std::time::{Duration, Instant};
use polars::prelude::*;
use polars::prelude::ParquetReader;


fn main() -> Result<()>{

    let start = Instant::now();
    let mut file = std::fs::File::open("cases_data.parquet").unwrap();
    let mut dataset = ParquetReader::new(&mut file).finish().unwrap();
    // Keep relevant columns and create new ones
    let dataset = dataset.lazy().select([
        col("case_id"),
        col("incoming"),
        col("outgoing"),
        col("event_name").str().contains("mediation").alias("mediation"),
        col("event_name").str().contains("cases_automator").alias("cases_automator"),
        col("event_name").str().contains("cases_massive").alias("cases_massive"),
    ]);

    // Aggregate the data
    let agg_dataset = dataset.groupby(["case_id"])
        .agg([
            col("incoming").sum().alias("incoming_sum"),
            col("outgoing").sum().alias("outgoing_sum"),
            col("mediation").sum().alias("med_sum"),
            col("cases_automator").sum().alias("ca_sum"),
            col("cases_massive").sum().alias("cm_sum"),
            col("incoming").first().alias("incoming_first"),
            col("outgoing").first().alias("outgoing_first"),
            col("mediation").first().alias("med_first"),
            col("cases_automator").first().alias("ca_first"),
            col("cases_massive").first().alias("cm_first"),
            col("incoming").last().alias("incoming_last"),
            col("outgoing").last().alias("outgoing_last"),
            col("mediation").last().alias("med_last"),
            col("cases_automator").last().alias("ca_last"),
            col("cases_massive").last().alias("cm_last"),
        ]).collect()?;

    println!("{}", agg_dataset.head(Some(5)));
    let duration = start.elapsed();
    println!("Time elapsed in processing the data is: {:?}", duration);
    return Ok(());
}
