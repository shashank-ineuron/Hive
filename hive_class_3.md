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
