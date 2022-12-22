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
        col("CAS_CASE_ID"),
        col("FLAG_INCOMING_GESTION"),
        col("FLAG_OUTGOING_GESTION"),
        col("CI_EVENT_NAME").str().contains("mediation").alias("mediation"),
        col("CI_EVENT_NAME").str().contains("cases_automator").alias("cases_automator"),
        col("CI_EVENT_NAME").str().contains("cases_massive").alias("cases_massive"),
    ]);

    // Aggregate the data
    let agg_dataset = dataset.groupby(["CAS_CASE_ID"])
        .agg([
            col("FLAG_INCOMING_GESTION").sum().alias("FIG_sum"),
            col("FLAG_OUTGOING_GESTION").sum().alias("FOG_sum"),
            col("mediation").sum().alias("med_sum"),
            col("cases_automator").sum().alias("ca_sum"),
            col("cases_massive").sum().alias("cm_sum"),
            col("FLAG_INCOMING_GESTION").first().alias("FIG_first"),
            col("FLAG_OUTGOING_GESTION").first().alias("FOG_first"),
            col("mediation").first().alias("med_first"),
            col("cases_automator").first().alias("ca_first"),
            col("cases_massive").first().alias("cm_first"),
            col("FLAG_INCOMING_GESTION").last().alias("FIG_last"),
            col("FLAG_OUTGOING_GESTION").last().alias("FOG_last"),
            col("mediation").last().alias("med_last"),
            col("cases_automator").last().alias("ca_last"),
            col("cases_massive").last().alias("cm_last"),
        ]).collect()?;

    println!("{}", agg_dataset.head(Some(5)));
    let duration = start.elapsed();
    println!("Time elapsed in processing the data is: {:?}", duration);
    return Ok(());
}
