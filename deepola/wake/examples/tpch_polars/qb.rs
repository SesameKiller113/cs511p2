use crate::utils::*;
extern crate wake;
use polars::prelude::*;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query:
// SELECT
//     s_name,
//     SUM(o_totalprice) AS total_order_value
// FROM
//     supplier,
//     nation,
//     region,
//     orders
// WHERE
//     s_nationkey = n_nationkey
//     AND n_regionkey = r_regionkey
//     AND r_name = 'EUROPE'
//     AND s_suppkey = o_custkey
// GROUP BY
//     s_name
// ORDER BY
//     total_order_value DESC;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Define columns for each table
    let table_columns = HashMap::from([
        ("supplier".into(), vec!["s_suppkey", "s_nationkey", "s_name"]),
        ("nation".into(), vec!["n_nationkey", "n_regionkey"]),
        ("region".into(), vec!["r_regionkey", "r_name"]),
        ("orders".into(), vec!["o_custkey", "o_totalprice"]),
    ]);

    // CSV reader nodes for each table
    let supplier_csvreader_node =
        build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node =
        build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node =
        build_csv_reader_node("region".into(), &tableinput, &table_columns);
    let orders_csvreader_node =
        build_csv_reader_node("orders".into(), &tableinput, &table_columns);

    // WHERE node for filtering region by r_name = 'EUROPE'
    let region_filter_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let mask = df.column("r_name").unwrap().equal("EUROPE").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN nodes to perform INNER JOIN for supplier, nation, region, and orders

    // Join nation and filtered region on n_regionkey = r_regionkey
    let join_nation_region = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();

    // Join supplier and nation on s_nationkey = n_nationkey
    let join_supplier_nation = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    // Join supplier_nation and orders on s_suppkey = o_custkey
    let join_supplier_orders = HashJoinBuilder::new()
        .left_on(vec!["s_suppkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();

    // AGGREGATE node for summing total order value and grouping by supplier name
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["s_name".to_string()])
        .set_aggregates(vec![("o_totalprice".into(), vec!["sum".into()])]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT and ORDER BY node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        let sorted_df = df
            .sort(&["s_name"], vec![true]) // Sort by 's_name' ascending
            .unwrap();
        // Rename 'o_totalprice_sum' to 'total_order_value'
        let total_order_value = sorted_df.column("o_totalprice_sum").unwrap().clone();
        let s_name = sorted_df.column("s_name").unwrap().clone();
        DataFrame::new(vec![
            s_name,
            Series::new("total_order_value", total_order_value),
        ])
        .unwrap()
    })))
    .build();


    // Connect nodes with subscriptions
    // Filter region
    region_filter_node.subscribe_to_node(&region_csvreader_node, 0); // Apply region filter first

    // Join nation and filtered region
    join_nation_region.subscribe_to_node(&nation_csvreader_node, 0); // Left input: nation
    join_nation_region.subscribe_to_node(&region_filter_node, 1); // Right input: filtered region

    // Join supplier and nation-region
    join_supplier_nation.subscribe_to_node(&supplier_csvreader_node, 0); // Left input: supplier
    join_supplier_nation.subscribe_to_node(&join_nation_region, 1); // Right input: nation-region

    // Join supplier_nation and orders
    join_supplier_orders.subscribe_to_node(&join_supplier_nation, 0); // Left input: supplier-nation
    join_supplier_orders.subscribe_to_node(&orders_csvreader_node, 1); // Right input: orders

    // Group and aggregate results
    groupby_node.subscribe_to_node(&join_supplier_orders, 0);

    // Sort results
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(supplier_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(region_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(region_filter_node);
    service.add(join_nation_region);
    service.add(join_supplier_nation);
    service.add(join_supplier_orders);
    service.add(groupby_node);
    service.add(select_node);

    service
}
