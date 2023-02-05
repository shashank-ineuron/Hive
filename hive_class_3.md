# create csv table for sales data

create table sales_order_data_csv_v1
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1")
; 

load data local inpath 'file:///home/hadoop/sales_order_data.csv' into table sales_order_data_csv_v1;

# load sales_order_data.csv data into above mentioned tables

create table sales_order_data_orc
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;

from sales_order_data_csv_v1 insert overwrite table sales_order_data_orc select *;

# Run a query to check map-reduce activity
select year_id, sum(sales) as total_sales from sales_order_data_orc group by year_id; 

# Hive properties to set specific number for reducers
In order to change the average load for a reducer (in bytes):                                                                                 
  set hive.exec.reducers.bytes.per.reducer=<number>                                                                                           
In order to limit the maximum number of reducers:                                                                                             
  set hive.exec.reducers.max=<number>                                                                                                         
In order to set a constant number of reducers:                                                                                                
  set mapreduce.job.reduces=<number> 

# set this property if doing static partition
set hive.mapred.mode=strict;

# create table command for partition tables - for Static

create table sales_data_static_part                                                                                                     
    (                                                                                                                                       
    ORDERNUMBER int,                                                                                                                        
    QUANTITYORDERED int,                                                                                                                    
    SALES float,                                                                                                                            
    YEAR_ID int                                                                                                                             
    )                                                                                                                                       
    partitioned by (COUNTRY string); 
    
# load data in static partition

insert overwrite table sales_data_static_part partition(country = 'USA') select ordernumber,quantityordered,sales,year_id from sales_ord
er_data_orc where country = 'USA';

# set this property for dynamic partioning
set hive.exec.dynamic.partition.mode=nonstrict;   


hive> create table sales_data_dynamic_part                                                                                                    
    (
    ORDERNUMBER int,                                                                                                                        
    QUANTITYORDERED int,                                                                                                                    
    SALES float,                                                                                                                            
    YEAR_ID int                                                                                                                             
    )
    partitioned by (COUNTRY string); 

# load data in dynamic partition table

insert overwrite table sales_data_dynamic_part partition(country) select ordernumber,quantityordered,sales,year_id,country from sales_or
der_data_orc;
  

# multilevel partition

create table sales_data_dynamic_multilevel_part_v1                                                                                      
    (
    ORDERNUMBER int,                                                                                                                        
    QUANTITYORDERED int,                                                                                                                    
    SALES float                                                                                                                             
    )
    partitioned by (COUNTRY string, YEAR_ID int); 
    
# load data in multilevel partitions

insert overwrite table sales_data_dynamic_multilevel_part_v1 partition(country,year_id) select ordernumber,quantityordered,sales,country
,year_id from sales_order_data_orc;
