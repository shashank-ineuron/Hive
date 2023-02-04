create table sales_data_v2
    (
    p_type string,
    total_sales int
    )
    row format delimited
    fields terminated by ',';

load data local inpath 'file:///config/workspace/sales_data_raw.csv' into table sales_data_v2;

# Command to create a new table from different table
create table sales_data_v2_bkup as select * from sales_data_v2;
