use crate::utils::*;
extern crate wake;
use polars::prelude::*;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

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
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "supplier".into(),
            vec!["s_name", "s_nationkey", "s_suppkey"],
        ),
        (
            "nation".into(),
            vec!["n_nationkey", "n_regionkey"],
        ),
        (
            "region".into(),
            vec!["r_regionkey", "r_name"],
        ),
        (
            "orders".into(),
            vec!["o_totalprice", "o_custkey"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let supplier_csvreader_node = build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_csv_reader_node("region".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);

    // WHERE node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("r_name").unwrap();
            let mask = a.equal("EUROPE").unwrap();
            let result = df.filter(&mask).unwrap();
            result
        })))
        .build();
    
    // HASH JOIN Node
    let join_supplier_nation = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();
    let join_nation_region = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();
    let join_supplier_orders = HashJoinBuilder::new()
        .left_on(vec!["s_suppkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();
    
    // GROUP BY Aggregate Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["s_name".to_string()])
        .set_aggregates(vec![
            ("o_totalprice".into(), vec!["sum".into()]),
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();
    
    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec![
                Series::new("s_name", df.column("s_name").unwrap()),
                Series::new("total_order_value", df.column("o_totalprice_sum").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["total_order_value"], vec![true])
                .unwrap()
        })))
        .build();
    
    // Connect nodes with subscription
    where_node.subscribe_to_node(&region_csvreader_node, 0);

    // join nation and region
    join_nation_region.subscribe_to_node(&nation_csvreader_node, 0);
    join_nation_region.subscribe_to_node(&where_node, 1);

    // join supplier and nation
    join_supplier_nation.subscribe_to_node(&supplier_csvreader_node, 0);
    join_supplier_nation.subscribe_to_node(&join_nation_region, 1);

    // join supplier and order
    join_supplier_orders.subscribe_to_node(&join_supplier_nation, 0);
    join_supplier_orders.subscribe_to_node(&orders_csvreader_node, 1);

    groupby_node.subscribe_to_node(&join_supplier_orders, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(region_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(where_node);
    service.add(join_nation_region);
    service.add(join_supplier_nation);
    service.add(join_supplier_orders);
    service.add(groupby_node);
    service.add(select_node);

    service

}
