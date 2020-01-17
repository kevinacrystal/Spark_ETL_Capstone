# Spark_ETL_Capstone
Spark application to perform ETL processes.
  teao========================================================================o
WELCOME TO MY CASE STUDY!     

Program Architecture
-------	------------
The program consists of four modules:
	1) Part1_SparkSQL.py
	2) Part2_SparkStreaming.py
	3) Part3_Analysis_and_Visualization.py
	4) MainEntryPoint.py

The first module, Part1_SparkSQL.py, consists of three user-defined functions wrapped in a main function that runs these functions sequentially when called.
The three functions are 'extract_from_table', 'transform_data', and 'load_into_mongo'.
'extract_from_table' uses PySpark to extract data from a relational database, MariaDB, and returns the data as a SparkSQL dataframe.
'transform_data' uses PySpark SQL query language to transform the dataframe according to the specification of the case study mapping document (Mapping Documnt.xlsx), and returns the transformed dataframe.
The transformation query for each respective table is stored in the module as a string variable, and must be passed in to the function as an argument.
'load_into_mongo' uses Spark to load the transformed data into MongoDB.

The second module, Part2_SparkStreaming.py, consists of four user-defined functions wrapped in a main function that runs these functions sequentially when called.
The four functions are 'kafka_producer', 'make_filtered_value_rdd', 'transform_rdd', and 'load_into_mongo'.
'kafka_producer' uses Requests to extract data from a specified URL and then instantiates a KafkaProducer to write the data to a Kafka topic.
'make_filtered_value_rdd' uses Spark Streaming to read data from the Kafka topic and write to an in-memory table. 
This table is converted to an RDD and transformations are applied to make the data usable. The usable RDD is returned.
'transform_rdd' maps the data to row schema specified as an argument to the function. The row schema for the respective tables are stored as variables in the module.
The RDD, now with schema, is converted back into a dataframe, which is returned.
'load_into_mongo' uses spark to load the dataframe into MongoDB.

The third module, Part3_Analysis_and_Visualization, uses pymongo to connect to MongoDB, and uses Pandas to analyze and visualize the data.

The fourth module, MainEntryPoint.py, creates a front-end application that offers users a variety of possible inputs. Based on the user input, the various tables in the two databases can be read and written to MongoDB.
Once the data has been written to MongoDB, a variety of analysis and visualizations can be performed. 


Testing Protocol
-------	--------

*Start Kafka Zookeeper
*Start Kafka Broker
	*Confirm that no Kafka topics exist   
*Open Command Prompt
	*Run command: python C:\Users\perscholas_student\eclipse-workspace_new\case_study\MainEntryPoint.py
*Run the program as directed below to test functional requirements:


Functional Requirement 1.1 & 1.2 
---------------------- ---------
1a)
Select option a to read and write credit card data
Select option a for CDW_SAPP_BRANCH

1b)
Select option a to read and write credit card data
Select option b for CDW_SAPP_CREDITCARD

1c)
Select option a to read and write credit card data
Select option c for CDW_SAPP_CUSTOMER

**All credit card data should now be viewable in MongoDB**


Functional Requirements 2.1, 2.2, 2.3
----------------------- -------------
2a)
Select option b to read and write Health Insurance Marketplace data
Select option a for BenefitsCostSharing.txt

2b)
Select option b to read and write Health Insurance Marketplace data
Select option b for Insurance.csv

2c)
Select option b to read and write Health Insurance Marketplace data
Select option c for PlanAttributes.csv

2d)
Select option b to read and write Health Insurance Marketplace data
Select option d for Network.csv

2e)
Select option b to read and write Health Insurance Marketplace data
Select option e for ServiceArea.csv

**All Health Insurance Marketplace data should now be viewable in MongoDB**


Functional Requirement 2.4
---------------------- ---
3a)
Select option c to analyze and/or visualize data
Select option a to plot state counts of ServiceAreaName, SourceName, and BusinessYear 

3b)
Select option c to analyze and/or visualize data
Select option b to plot the counts of sources across the country  

3d)
Select option c to analyze and/or visualize data
Select option d to plot the number of benefit plans in each state 

3e)
Select option c to analyze and/or visualize data
Select option e to print the number of mothers who smoke

3f)
Select option c to analyze and/or visualize data
Select option f to plot the rate of smoking for each region


*Select option d to log out of the program.

o========================================================================o