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

# download hive catalog jar file , if serde libraries are not imported

https://repo1.maven.org/maven2/org/apache/hive/hcatalog/hive-hcatalog-core/0.14.0/

# run add jar command on hive shell
add jar file:///config/workspace/hive-hcatalog-core-0.14.0.jar;

# create json table

create table json_table                                                                                                                 
    ( name string,                                                                                                                          
    id int,                                                                                                                                 
    skills array<string>                                                                                                                    
    )                                                                                                                                       
    row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'                                                                              
    stored as textfile; 
    
# load data into json

load data local inpath 'file:///config/workspace/json_file.json' into table json_table;
    
    
# create a table which will store data in parquet

create table sales_data_pq_final                                                                                                        
    (                                                                                                                                       
    product_type string,                                                                                                                    
    total_sales int                                                                                                                         
    )                                                                                                                                       
    stored as parquet;  
    
# load data in parquet file
from sales_data_v2 insert overwrite table sales_data_pq_final select *;
