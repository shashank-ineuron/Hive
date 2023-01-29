show databases;

create database hive_db;

use hive_db;

create table department_data                                                                                                            
    (                                                                                                                                       
    dept_id int,                                                                                                                            
    dept_name string,                                                                                                                       
    manager_id int,                                                                                                                         
    salary int)                                                                                                                             
    row format delimited                                                                                                                    
    fields terminated by ','; 
    
describe department_data;

describe formatted department_data;

# For data load from local
load data local inpath 'file:///config/workspace/depart_data.csv' into table department_data; 

# Display column name
set hive.cli.print.header = true;

drop table department_data;

create table department_data                                                                                                            
    (                                                                                                                                       
    dept_id int,                                                                                                                            
    dept_name string,                                                                                                                       
    manager_id int,                                                                                                                         
    salary int)                                                                                                                             
    row format delimited                                                                                                                    
    fields terminated by ','; 

# Load data from hdfs location
load data inpath '/tmp/depart_data.csv' into table department_data;


# Command to create external tables
create external table department_date_external
    (
    dept_id int,
    dept_name string,
    manager_id int,
    salary int
    )
    row format delimited
    fields terminated by ','
    location '/input_data/';
    
 # work with Array data types

create table employee                                                                                                                   
    (                                                                                                                                       
    id int,                                                                                                                                 
    name string,                                                                                                                            
    skills array<string>                                                                                                                    
    )                                                                                                                                       
    row format delimited                                                                                                                    
    fields terminated by ','                                                                                                                
    collection items terminated by ':';                                                                                                     

load data local inpath 'file:///config/workspace/array_data.csv' into table employee; 


# Get element by index in hive array data type

select id, name, skills[0] as prime_skill from employee;

select                                                                                                                                  
    id,                                                                                                                                     
    name,                                                                                                                                   
    size(skills) as size_of_each_array,                                                                                                     
    array_contains(skills,"HADOOP") as knows_hadoop,                                                                                        
    sort_array(skills) as sorted_array                                                                                                                 
    from employee; 
