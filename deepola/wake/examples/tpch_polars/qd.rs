use crate::utils::*;
extern crate wake;
use polars::prelude::*;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

// select
// 	sum(l_extendedprice* (1 - l_discount)) as revenue
// from
// 	lineitem,
// 	part
// where
//     p_partkey = l_partkey
//     and l_shipinstruct = 'DELIVER IN PERSON'
//     and
//     (
//         (
//             p_brand = 'Brand#12'
//             and l_quantity >= 1 and l_quantity <= 1 + 10
//             and p_size between 1 and 5
//         )
//         or
//         (
//             p_brand = 'Brand#23'
//             and l_quantity >= 10 and l_quantity <= 10 + 10
//             and p_size between 1 and 10
//         )
//         or
//         (
//             p_brand = 'Brand#34'
//             and l_quantity >= 20 and l_quantity <= 20 + 10
//             and p_size between 1 and 15
//         )
//     )

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_extendedprice", "l_discount", "l_partkey", "l_shipinstruct", "l_quantity"],
        ),
        (
            "part".into(),
            vec!["p_partkey", "p_brand", "p_size"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // l_shipinstruct = 'DELIVER IN PERSON'
            let a = df.column("l_shipinstruct").unwrap();
            let mask = a.equal("DELIVER IN PERSON").unwrap();

            // AND condition with ORs
            let brand = df.column("p_brand").unwrap();
            let quantity = df.column("l_quantity").unwrap();
            let size = df.column("p_size").unwrap();

            // First OR condition
            let brand12_mask = brand.equal("Brand#12").unwrap()
                & quantity.gt_eq(1).unwrap() & quantity.lt_eq(11).unwrap()
                & size.gt_eq(1).unwrap() & size.lt_eq(5).unwrap();

            // Second OR condition
            let brand23_mask = brand.equal("Brand#23").unwrap()
                & quantity.gt_eq(10).unwrap() & quantity.lt_eq(20).unwrap()
                & size.gt_eq(1).unwrap() & size.lt_eq(10).unwrap();

            // Third OR condition
            let brand34_mask = brand.equal("Brand#34").unwrap()
                & quantity.gt_eq(20).unwrap() & quantity.lt_eq(30).unwrap()
                & size.gt_eq(1).unwrap() & size.lt_eq(15).unwrap();
            
            // Combine the ORs
            let combined_mask = brand12_mask | brand23_mask | brand34_mask;

            let final_mask = mask & combined_mask;
            let result = df.filter(&final_mask).unwrap();
            result
        })))
        .build();

    // HASH JOIN Node
    let join_lineitem_part = HashJoinBuilder::new()
        .left_on(vec!["p_partkey".into()])
        .right_on(vec!["l_partkey".into()])
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![
                Series::new(
                    "revenue",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                ),
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    // GROUP BY Aggregate Node
    let sum_accumulator = SumAccumulator::new();

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec![
                Series::new("revenue", df.column("revenue").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
        })))
        .build();
    
    // Connect nodes with subscription
    join_lineitem_part.subscribe_to_node(&part_csvreader_node, 0);
    join_lineitem_part.subscribe_to_node(&lineitem_csvreader_node, 1);

    where_node.subscribe_to_node(&join_lineitem_part, 0);

    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(part_csvreader_node);
    service.add(where_node);
    service.add(join_lineitem_part);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
