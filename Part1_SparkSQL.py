from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Transformation queries making changes specified in the mapping document
branch_transformation_query = 'select \
        BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, BRANCH_STATE,\
        cast(IFNULL(BRANCH_ZIP, "99999") as char(7)) as BRANCH_ZIP, \
        concat("(", left(branch_phone, 3), ")", substring(branch_phone, 4, 3), "-", right(branch_phone, 4)) as BRANCH_PHONE, \
        LAST_UPDATED \
        from tempView'
cc_transformation_query = 'select \
        credit_card_no as CUST_CC_NO, \
        concat(year, \
            case when month < 10 then concat("0", month) else month end, \
            case when day < 10 then concat("0", day) else day end) as TIMEID, \
        CUST_SSN, BRANCH_CODE, TRANSACTION_TYPE, \
        TRANSACTION_VALUE, TRANSACTION_ID \
        from tempView'
customer_transformation_query = 'select \
        initcap(first_name) as CUST_F_NAME, \
        lower(middle_name) as CUST_M_NAME, \
        initcap(last_name) as CUST_L_NAME, \
        ssn as CUST_SSN, \
        credit_card_no as CUST_CC_NO, \
        concat(apt_no, " ", street_name) as CUST_STREET, \
        CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, \
        concat(left(cust_phone, 3), "-", right(cust_phone, 4)) as CUST_PHONE, \
        CUST_EMAIL, LAST_UPDATED \
        from tempView'

# Extract data from MariaDB table and return dataframe
def extract_from_table(maria_db_table):
    extracted_df = spark.read \
        .format('jdbc') \
        .options(
        url= 'jdbc:mysql://localhost:3306/cdw_sapp',
        driver = 'com.mysql.cj.jdbc.Driver',
        dbtable= maria_db_table,
        user= 'root',
        password= '').load()
    #print(f'Data extracted from MariaDB table: {maria_db_table}')
    return extracted_df

# Perform transformations on extracted data using SparkSQL
# using a specified transformation query to make changes 
# according to specification found in the mapping document
def transform_data(extracted_df, transformation_query):
    extracted_df.createOrReplaceTempView('tempView')
    transformation_table = spark.sql(transformation_query)
    print('Data transformed with SparkSQL')
    return transformation_table

# Load transformed data into MongoDB using Spark
def load_into_mongo(transformation_table, target_collection):
    transformation_table.write\
        .format('mongo') \
        .mode('append') \
        .option('database', 'credit_card_data') \
        .option('collection', target_collection) \
        .option('uri', 'mongodb://127.0.0.1/') \
        .save()
    print(f'\nTransformed data sent to Mongo collection: {target_collection}')

# Run all of the above functions on a specified table, with
# a specified mapping query, and a specified target collection
def main(maria_db_table, transformation_query, target_collection):
    extracted_df = extract_from_table(maria_db_table)
    transformed_df = transform_data(extracted_df, transformation_query)
    load_into_mongo(transformed_df, target_collection)
    
if __name__ == '__main__':
    main()

