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

# create table as CSV SerDe
create table csv_table                                                                                                                  
    (                                                                                                                                       
    name string,                                                                                                                            
    location string                                                                                                                         
    )                                                                                                                                       
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'                                                                           
    with serdeproperties (                                                                                                                  
     "separatorChar" = ",",                                                                                                                 
     "quoteChar" = "\"",                                                                                                                    
     "escapeChar" = "\\"                                                                                                                    
     )                                                                                                                                       
    stored as textfile                                                                                                                      
    tblproperties ("skip.header.line.count" = "1"); 
    
# load data from local
load data local inpath 'file:///config/workspace/csv_file.csv' into table csv_table;
