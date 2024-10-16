// TODO: You need to implement the query c.sql in this file.
use crate::utils::*;
extern crate wake;
use polars::prelude::*;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Define columns for each table based on the query
    let table_columns = HashMap::from([
        (
            "customer".into(),
            vec!["c_custkey", "c_name", "c_acctbal"],
        ),
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_orderdate"],
        ),
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_extendedprice", "l_discount"],
        ),
    ]);

    // CSV reader nodes for each table
    let customer_csvreader_node =
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);
    let orders_csvreader_node =
        build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE node for filtering orders by order date
    let orders_filter_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Define the date range
            let start_date = "1993-10-01";
            let end_date = "1994-01-01"; // '1993-10-01' + INTERVAL '3' MONTH

            let order_date = df.column("o_orderdate").unwrap();
            let mask = order_date
                .utf8()
                .unwrap()
                .into_iter()
                .map(|date_opt| {
                    date_opt.map_or(false, |date_str| {
                        date_str >= start_date && date_str < end_date
                    })
                })
                .collect::<BooleanChunked>();

            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN nodes to perform INNER JOINs
    // Join orders and filtered orders on o_custkey = c_custkey
    let join_customer_orders = HashJoinBuilder::new()
        .left_on(vec!["c_custkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();

    // Join orders_lineitem and lineitem on o_orderkey = l_orderkey
    let join_orders_lineitem = HashJoinBuilder::new()
        .left_on(vec!["o_orderkey".into()])
        .right_on(vec!["l_orderkey".into()])
        .build();

    // EXPRESSION Node to compute l_extendedprice * (1 - l_discount)
    let revenue_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap().cast(&DataType::Float64).unwrap();
            let discount = df.column("l_discount").unwrap().cast(&DataType::Float64).unwrap();
            let one_minus_discount = &discount * -1.0 + 1.0;
            let revenue = &extended_price * &one_minus_discount;
            let revenue_series = Series::new("revenue", revenue);
            df.hstack(&[revenue_series]).unwrap()
        })))
        .build();

    // AGGREGATE node for summing revenue and grouping by c_custkey, c_name, c_acctbal
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec![
            "c_custkey".to_string(),
            "c_name".to_string(),
            "c_acctbal".to_string(),
        ])
        .set_aggregates(vec![("revenue".into(), vec!["sum".into()])]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT and ORDER BY node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        // Sort by 'revenue_sum' descending, then by 'c_custkey' ascending
        let sorted_df = df
            .sort(&["revenue_sum", "c_custkey"], vec![false, true])
            .unwrap();
        
        // Rename 'revenue_sum' to 'revenue' and select columns in the correct order
        let c_custkey = sorted_df.column("c_custkey").unwrap().clone();
        let c_name = sorted_df.column("c_name").unwrap().clone();
        let c_acctbal = sorted_df.column("c_acctbal").unwrap().clone();
        let revenue = sorted_df.column("revenue_sum").unwrap().clone();
        
        DataFrame::new(vec![
            c_custkey,
            c_name,
            c_acctbal,
            Series::new("revenue", revenue),
        ])
        .unwrap()
    })))
    .build();

    // Connect nodes with subscriptions
    // Filter orders by date
    orders_filter_node.subscribe_to_node(&orders_csvreader_node, 0);

    // Join customer and filtered orders
    join_customer_orders.subscribe_to_node(&customer_csvreader_node, 0); // Left input: customer
    join_customer_orders.subscribe_to_node(&orders_filter_node, 1); // Right input: filtered orders

    // Join orders_lineitem and lineitem
    join_orders_lineitem.subscribe_to_node(&join_customer_orders, 0); // Left input: customer-orders
    join_orders_lineitem.subscribe_to_node(&lineitem_csvreader_node, 1); // Right input: lineitem

    // Compute revenue
    revenue_node.subscribe_to_node(&join_orders_lineitem, 0);

    // Group and aggregate results
    groupby_node.subscribe_to_node(&revenue_node, 0);

    // Select and sort results
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(customer_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(orders_filter_node);
    service.add(join_customer_orders);
    service.add(join_orders_lineitem);
    service.add(revenue_node);
    service.add(groupby_node);
    service.add(select_node);

    service
}
