use crate::utils::*;

extern crate wake;
use polars::prelude::*;
use wake::graph::*;
use wake::polars_operations::{AccumulatorNode, AppenderNode, MapAppender, SumAccumulator};

use std::collections::HashMap;
use std::ops::Not;

/// This node implements the following SQL query:
// SELECT
//     sum(p_retailprice) AS total
// FROM
//     part
// WHERE
//     p_retailprice > 1000
//     AND p_size IN (5, 10, 15, 20)
//     AND p_container <> 'JUMBO JAR'
//     AND p_mfgr LIKE 'Manufacturer#1%';

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "part".into(),
        vec!["p_retailprice", "p_size", "p_container", "p_mfgr"],
    )]);

    // CSVReaderNode would be created for this table.
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let price_mask = df.column("p_retailprice").unwrap().gt(1000.0).unwrap();
            let size_mask = df
                .column("p_size")
                .unwrap()
                .i64()
                .unwrap()
                .into_iter()
                .map(|opt_size| {
                    opt_size
                        .map(|size| [5, 10, 15, 20].contains(&(size as i32)))
                        .unwrap_or(false)
                })
                .collect::<BooleanChunked>();
            let container_mask = df
                .column("p_container")
                .unwrap()
                .equal("JUMBO JAR")
                .unwrap()
                .not();
            let mfgr_mask = df
                .column("p_mfgr")
                .unwrap()
                .utf8()
                .unwrap()
                .into_iter()
                .map(|x| x.map(|s| s.starts_with("Manufacturer#1")).unwrap_or(false))
                .collect::<BooleanChunked>();

            let combined_mask = price_mask & size_mask & container_mask & mfgr_mask;
            let filtered_df = df.filter(&combined_mask).unwrap();

            // Add a dummy group key column with a constant value
            let all_col = Series::new("all", vec![1; filtered_df.height()]);
            filtered_df.hstack(&[all_col]).unwrap()
        })))
        .build();

    // AGGREGATE Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["all".to_string()]) // Group by the dummy column
        .set_aggregates(vec![("p_retailprice".into(), vec!["sum".into()])]);

    let aggregate_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Extract the sum and rename it to 'total'
            let total = df.column("p_retailprice_sum").unwrap();
            DataFrame::new(vec![Series::new("total", total)]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&part_csvreader_node, 0);
    aggregate_node.subscribe_to_node(&where_node, 0);
    select_node.subscribe_to_node(&aggregate_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(part_csvreader_node);
    service.add(where_node);
    service.add(aggregate_node);
    service.add(select_node);

    service
}
