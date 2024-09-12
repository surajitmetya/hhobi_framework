# 1. Importing required libraries

from db_tokenization.tokenization.tokens import Tokenizer
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import min, max  
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile
import pandas as pd
import json
from amsession import client
from amsession import Session
from Constants import *
import base64
# 2. Function Modules
# 2.1.1 Fetch the control table entries for the run by pipeline runtime

# This function will query load control table where Load_Start_Time matches with pipeline start time.
def fetch_data_flows(spark):
    batch_time = (datetime.now() + timedelta(minutes = 330)).hour
    
    query = "select * from prod_hhobi.hhobi_audit.ingestion_load_control_table where targetLayer = 'Raw' and jobSchedule['Load_Start_Time'] = '{}' ".format(str(batch_time))
    matching_records = spark.sql(query)

    return matching_records

# 2.1.2 Fetch the control table entries for the run

# SQL query to select all entries from the "ingestion_load_control_table" table where isActive = 'Y'
def get_load_control_table_entries(spark, dataGroupId = None, dataFlowGroup = None, dataFlowId = None):
    query = "SELECT * FROM {} WHERE isActive = 'Y'".format(load_control_table)
    if dataGroupId is not None:
        query += f" AND dataGroupId = '{dataGroupId}'" 
    if dataFlowGroup is not None:
        query += f" AND dataFlowGroup = '{dataFlowGroup}'"
    if dataFlowId is not None:
        query += f" AND dataFlowId = '{dataFlowId}'"
    response_df = spark.sql(query)
    return response_df

def column_native_encrypt(df_enc, Tokenizationdetails):
    try:
        schema_source=df_enc.schema.fields
        session = Session(group="dataops")
        t = Tokenizer(session=session)
        Tokenizationdetails=Tokenizationdetails.split(",")
        for column in Tokenizationdetails:
            if (len(Tokenizationdetails)==1 and Tokenizationdetails[0]=="NA") or len(Tokenizationdetails)==0:
                return df_enc
            elif column in df_enc.columns:
                df_enc = df_enc.withColumn(f"{column}_encrypted", expr(f"main.tokenizer.Encrypt({column})").cast(StringType())).drop(column)
                # encrypted_col_list = t.tokens(col_list, data_class=classtype)
                df_enc=df_enc.withColumnRenamed(f"{column}_encrypted",column)
            else:
                raise Exception()
        df_enc = df_enc.select(*[col(x.name) for x in schema_source])
    except Exception as e:
        print('invalid entry, Exception: ', e)
    
    return df_enc
# 2.2. Fetch the Entity table entries

# SQL query to Select all entries from the "ingestion_entities" table where isActive = 'Y' and dataFlowFK provided
def get_entities_table_entries(spark, data_flow_FK):
    query = "SELECT * FROM {} WHERE dataFlowFK ='{}' and isActive = 'Y' order by jobId".format(ingestion_entity_table, data_flow_FK)
    response_df = spark.sql(query)
    return response_df

def update_ingestion_entities_status(spark, status, dataFlowFK, jobID):
    if dataFlowFK is not None:
        query = "update prod_hhobi.hhobi_audit.ingestion_entities set status = '{}' where dataFlowFK = '{}'".format(status, dataFlowFK)
    else:
        query = "update prod_hhobi.hhobi_audit.ingestion_entities set status = '{}' where JobId = {}".format(status, jobID)
    print(query)
    spark.sql(query)

def table_dependency_check(spark, dependencies):
    print("Dependencies " , dependencies)
    if dependencies is None:
        return True
    
    dependency_tuple = tuple(map(int, dependencies.split(',')))
    if len(dependency_tuple) == 1:
        query = "select distinct status from prod_hhobi.hhobi_audit.ingestion_entities where JobId = {} and status = 'Completed'".format(dependency_tuple[0])
    else:
        query = "select distinct status from prod_hhobi.hhobi_audit.ingestion_entities where JobId in {} and status = 'Completed'".format(dependency_tuple)
    df = spark.sql(query).collect()
    
    if len(df) == 1 and df[0][0] == "Completed":
        return True
    return False

# 2.3. Initialization Module

# This Function initializes the entities tables into list of python dictionaries.
def intialize_tables(spark, row): 
    global sourceFormat, sourceDetails, quality
    pk_column_value = row['dataGroupId'] + "-" + row['dataFlowGroup'] + "-" + row['dataFlowId']
    sourceFormat = row['sourceFormat']
    sourceDetails = row['sourceDetails']

    matching_records_table = get_entities_table_entries(spark, pk_column_value).collect()
    update_ingestion_entities_status(spark, "Running", pk_column_value, None)
    table_details = []
    for entry in matching_records_table:
        table_details_dict = {}
        table_details_dict['dataFlowFK'] = entry.dataFlowFK
        table_details_dict['JobId'] = entry.JobId
        table_details_dict['targetTablename'] = entry.targetTablename
        table_details_dict['loadDependency'] = entry.loadDependency
        table_details_dict['primaryKey'] = entry.primaryKey
        table_details_dict['tableProperties'] = entry.tableProperties
        table_details_dict['tableSchema'] = entry.tableSchema
        table_details_dict['partitionColumns'] = entry.partitionColumns
        table_details_dict['SourceTableName'] = entry.SourceTableName
        table_details_dict['loadLogic'] = entry.loadLogic
        table_details_dict['AdditionalConfiguration'] = entry.AdditionalConfiguration
        table_details_dict['loadType'] = entry.loadType
        table_details_dict['cdcLogic'] = entry.cdcLogic
        table_details_dict['cdcConfiguration'] = entry.cdcConfiguration
        table_details_dict['DetokenizationToeknizationFlag'] = entry.DetokenizationToeknizationFlag
        table_details_dict['Tokenizationdetails'] = entry.Tokenizationdetails
        table_details_dict['Detokenization_details'] = entry.Detokenization_details
        table_details_dict['isActive'] = entry.isActive
        table_details_dict['archivalPolicyDays'] = entry.archivalPolicyDays
        table_details_dict['vacuumPolicy_Weeks'] = entry.vacuumPolicy_Weeks
        table_details_dict['errorHandling'] = entry.errorHandling
        table_details_dict['StatusTas'] = entry.StatusTas
        table_details_dict['splitLoad'] = entry.splitLoad
        table_details_dict['splitLoadConfigurations'] = entry.splitLoadConfigurations
        table_details_dict['status'] = entry.status
        table_details_dict['native_encryption_flag']=entry.native_encryption_flag
        table_details_dict['native_encryption_columns']=entry.native_encryption_columns
        table_details.append(table_details_dict)

    return table_details

# 2.4. Fetch credentials from the AWS secrets manager

# This function returns the secret value from AWS SecretsManager based on Secret Name and Region Name
def fetch_secrets(secret_name):
    session = Session(group="dataops")
    amsession_client = client(service="secretsmanager", session=session)
    response = amsession_client.get_secret_value(SecretId= secret_name)
    secret_string = response['SecretString']
    secret = json.loads(secret_string)
    return secret

# 2.5. Fetching the jdbc details from the AWS secret

# Function returns jdbc credentials - jdbcurl, driver, username and password
def get_jdbc_credentials(data):
    db_value = data.sourceConnectionDetails["db"]
    jdbc_driver_value = data.sourceConnectionDetails["jdbcDriver"]
    secret_name = data.sourceConnectionDetails["aws_secret_name"]
    secret = fetch_secrets(secret_name)
    username = secret['Service account']
    password = secret['password']
    port = secret['port']
    host = secret['host']
    url = f"jdbc:oracle:thin:@{host}:{port}/{db_value}"
    return url, jdbc_driver_value, username, password

# 2.6. Determine the fetch size of the source

# This Function returns dataframe based on Partition key
def get_df_by_key(spark, jdbcUrl, sql_query, username, password, jdbcDriver, fetchsize, partition_key = None, lower_bound = None, upper_bound = None, num_partitions = 16):
    print(sql_query)
    if not partition_key:
        df = spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("dbtable", sql_query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", jdbcDriver) \
            .option("fetchsize", fetchsize) \
            .load()	
    else:		
        df = spark.read.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("dbtable", sql_query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", jdbcDriver) \
            .option("partitionColumn", partition_key) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", num_partitions) \
            .option("fetchsize", fetchsize) \
            .load()	   
    return df

# 2.7. Read source data based on the refresh type and table size

# Function reads the JDBC table based on loadType and tablesize
def read_jdbc_table_by_table_type(spark, table_name, url, driver, username, password, primaryKeyList, partitionKey, source_table, target_table, mode, loadType, cdc_column, cdc_condition, tablesize, fetchsize, delta = 0):
    if loadType == "Full Refresh":
        query = ''' (select count(*) as total from {}) '''.format(table_name)
        response_df = get_df_by_key(spark, url, query, username, password, driver, fetchsize)
        upper_bound = int(response_df.select('total').collect()[0][0])
        query = " (select rownum, a.* FROM {} a ) ".format(table_name)
        response_df = get_df_by_key(spark, url, query, username, password, driver, fetchsize, 'rownum', 0, upper_bound, 3)
        df = response_df.drop(col("rownum"))
        return df

    else:
        query = "Select * from " + table_name
        if spark.catalog.tableExists(target_table):
            delta_Date, delta_Date_time, formatted_delta_date = find_last_updated_date(spark, table_name, cdc_column, delta)
            query = query + " where " + cdc_condition.format("'" + str(delta_Date_time) + "'", "'" + str(delta_Date_time) + "'")
        query = "(" + query + ")"
        response_df = get_df_by_key(spark, url, query, username, password, driver, fetchsize)
        return response_df
    
# 2.8. Tokenize the relevant columns based on the details provided

# This function will tokenize the dataframe columns based on Tokenizationdetails dictionary.
def tokenize_columns(spark, df, Tokenizationdetails):
    print("Tokenizing....")
    df_temp = df

    for column, classtype in Tokenizationdetails.items(): 

        col_list = [row[column] for row in df.select(column).distinct().collect()]

        if len(col_list) == 1 and col_list[0] == None:
            df = df.withColumn(column + "_tokenized", lit(None))
        elif len(col_list) == 1 and col_list[0] != None:
            session = Session(group="dataops")
            t = Tokenizer(session=session)
            encrypted_col_list = t.tokens(col_list, data_class=classtype)
            df = df.withColumn(column + "_tokenized", lit(encrypted_col_list[0]))
        elif len(col_list) > 1:
            session = Session(group="dataops")
            t = Tokenizer(session=session)
            encrypted_col_list = t.tokens(col_list, data_class=classtype)
            data = [(encrypted_col_list[i], col_list[i]) for i in range(len(col_list))]
            schema = StructType([StructField(column+"_tokenized", StringType(), True), StructField(column+"_original", StringType(), True)])
            df_new = spark.createDataFrame(data, schema)
            df = df.join(df_new, df[column] == df_new[column+"_original"], "left")
            # df = df.drop(column).withColumnRenamed(column+"_new", column)
            df = df.drop(column+"_original")
    df = df.select([col(c).cast("string").alias(c) if c.endswith('_tokenized') else col(c) for c in df.columns])
        
    # df = df.select(*[col(x.name) for x in df_temp.schema.fields])
    return df

# 2.9. Joining Key calculation

# This function generates the joining key for joining two tables based on the primary key. If there are more than 1 column as joining key, the input is given with columns separated by commas in the config table.
def generate_joining_key(primary_key):
    primary_key_list = primary_key.split(',') 
    keys = []
    for i in primary_key_list:
        keys.append('T.' + i + ' = S.' + i + ' ')
    joining_key = ''
    joining_key = 'and '.join(keys)
    return joining_key

# 2.10. Get the last loaded date of the table

# Function to find the last updated date for a given table, this is to get the delta logic to fetch from the source
def find_last_updated_date(spark, tablename, date_column, delta = 0):
    query = ''' select max({}) from {}{} '''.format(date_column, ingestion_schema, tablename) 
    max_Date = spark.sql(query).collect()[0][0]
    delta_Date = (max_Date - timedelta(days = int(delta)))
    delta_Date_time = datetime.combine(delta_Date, datetime.min.time())
    formatted_delta_date = delta_Date.strftime('%d-%b-%y').upper()
    return delta_Date, delta_Date_time, formatted_delta_date

# 2.11. Create target dataset based on the CDC mode of the table

# Writes the dataframe to target table based on mode type.
def write_data(spark, source_df, target_table, mode, primary_keys):
    
    source_df = source_df.select(*[
    col(c).cast("string").alias(c) if t == 'string' else col(c)
    for c, t in source_df.dtypes])

    print("inside Write data, source count =", source_df.count())

    if mode == overwrite:
        # Overwrite the target Delta table
        source_df = source_df.withColumn("Delta_Insert_Timestamp", F.current_timestamp())
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

        var_source_record_cnt=source_df.count()
        var_count_dest_inserted=0
        var_count_overall_health=0
        if var_source_record_cnt==0:
            var_count_overall_health=1
        df_hist=spark.sql(f"""describe history {target_table}""")
        df_hist_operation=df_hist.filter(df_hist.operation=="CREATE OR REPLACE TABLE AS SELECT").select(df_hist.operation,df_hist.operationMetrics.numOutputRows.alias('dest_count')).orderBy(df_hist.version.desc()).first()
        var_count_dest_inserted=int(df_hist_operation['dest_count'])
        spark.sql(f"""insert into prod_hhobi.hhobi_audit.ingestion_Load_Stats(table_name,data_time,action_type,source_records_count,insert_record_count,update_record_count,overall_health )values('{target_table}',current_timestamp(),'Overwrite',{var_source_record_cnt}, {var_count_dest_inserted},0,{var_count_overall_health})""")


    elif mode == append:
        # Append data to Delta table
        source_df = source_df.withColumn("year_month_code", date_format(source_df.IA_INSERT_DT, "yyyyMM"))
        source_df = source_df.withColumn("Delta_Insert_Timestamp", F.current_timestamp())
        source_df.write.format("delta").mode("append").saveAsTable(target_table)

        var_source_record_cnt=source_df.count()
        var_count_dest_inserted=0
        var_count_overall_health=0
        if var_source_record_cnt==0:
            var_count_overall_health=1
        df_hist=spark.sql(f"""describe history {target_table}""")
        df_hist_operation=df_hist.filter(df_hist.operation=='WRITE').select(df_hist.operation,df_hist.operationMetrics.numOutputRows.alias('dest_count')).orderBy(df_hist.version.desc()).first()
        var_count_dest_inserted=int(df_hist_operation['dest_count'])
        spark.sql(f"""insert into prod_hhobi.hhobi_audit.ingestion_Load_Stats(table_name,data_time,action_type,source_records_count,insert_record_count,update_record_count,overall_health )values('{target_table}',current_timestamp(),'append',{var_source_record_cnt}, {var_count_dest_inserted},0,{var_count_overall_health})""")

    elif mode == merge:
        if target_table != 'prod_hhobi.hhobi_in.IA_HP_SERIAL_NUM':
            source_df = source_df.withColumn("year_month_code", date_format(source_df.IA_INSERT_DT, "yyyyMM"))
        source_df = source_df.withColumn("Delta_Insert_Timestamp", F.current_timestamp())

        catalog_schema = ".".join(target_table.split(".")[0:-1]) + "."
        merge_table = catalog_schema + "merge_table"

        source_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable(merge_table)

        # Merge data into target Delta table from merge delta table
        if spark.catalog.tableExists(target_table):
            joining_key = generate_joining_key(primary_keys) 
            query = "MERGE INTO " + target_table + " as T using " + merge_table + " as S ON " + joining_key + " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
            merge_audit=spark.sql(query)
            var_count_dest_updated=merge_audit.first()['num_updated_rows']
            var_count_dest_inserted=merge_audit.first()['num_inserted_rows']
            var_count_dest_affected=merge_audit.first()['num_affected_rows']
            var_count_overall_health=0
            source_record_cnt=source_df.count()
            print(f"Adding Audit entry - Affected: {var_count_dest_affected} Inserted: {var_count_dest_inserted} Updated: {var_count_dest_updated}")
            if source_record_cnt==0:
                var_count_overall_health=1
            else:
                var_count_overall_health=var_count_dest_affected/source_record_cnt
            spark.sql(f'''insert into prod_hhobi.hhobi_audit.ingestion_Load_Stats
                  (table_name,data_time,action_type,source_records_count,insert_record_count,update_record_count,overall_health )values(
                  '{target_table}',current_timestamp(),'Merge',{source_record_cnt}, {var_count_dest_inserted},{var_count_dest_updated},{var_count_overall_health})''')
        else:
            source_df.write.format("delta").mode("append").saveAsTable(target_table) 
        
        drop_query = "DROP TABLE " + merge_table
        spark.sql(drop_query)
    print("Completed load for table "+ target_table, datetime.now())

# 2.12. Additional logic - will be replaced by a custom transformation module

# Populates the domain field based on CUST_HIER5_NAME column in IA_CUSTOMERS table.
def add_domain_field(df):
    def extract_domain(col):
        if col == None:
            return None
        return col.split("@")[-1]

    extract_domain_udf = udf(extract_domain, StringType())
    df = df.withColumn('Domain', extract_domain_udf(df['CUST_HIER5_NAME'])) 
    return df

# Updates Dataflows status fields indicating the message entered in the status parameter. can be 'Ready for DLT' or 'Completed' or 'In progress'
def update_dataflow_status(spark, dataFlowGroup, status):
    query = "update prod_hhobi.hhobi_audit.ingestion_load_control_table set DataFlow_status = '{}' where dataFlowGroup = '{}' and targetLayer in ('Bronze', 'Silver','Gold')".format(status, dataFlowGroup)
    spark.sql(query)

# This function will update the Dataflow_Status flag upon successful completion of delta table load.
def fetch_bronze_data_flows(spark):
    batch_time = (datetime.now() + timedelta(minutes = 330)).hour
    
    query = "select * from prod_hhobi.hhobi_audit.ingestion_load_control_table where targetLayer = 'Bronze' and DataFlow_Status = 'Ready for DLT'"  #.format(str(batch_time))
    matching_records = spark.sql(query)

    return matching_records

# This Function will update the Dataflow_status flag upon successful completion of bronze data load.
def update_bronze_dataflow_status(spark, dataFlowGroup, status):
    query = "update prod_hhobi.hhobi_audit.ingestion_load_control_table set DataFlow_status = '{}' where dataFlowGroup = '{}' and targetLayer in ('Bronze')".format(status, dataFlowGroup)
    spark.sql(query)

# 2.13. Write the tokenized data into the target tables

# This function calls tokenize_columns method followed by write_data
def tokenize_and_write_data(spark, source_data_df, table):
    
    if table['AdditionalConfiguration'] != "":
        Add_fields = eval(table['AdditionalConfiguration'])
        source_data_df = Add_fields(source_data_df)
        print("Adding domain completed")

    data_count = source_data_df.count()
    print("data_count =",data_count)
    if data_count < 1000000:
        if table['DetokenizationToeknizationFlag'] == 'Y':
            tokenized_df = tokenize_columns(spark, source_data_df, table['Tokenizationdetails'])
        else:
            tokenized_df = source_data_df
        if table['native_encryption_flag'] == 'Y':
            tokenized_df= column_native_encrypt(tokenized_df, table['native_encryption_columns'])
        write_data(spark, tokenized_df, table['targetTablename'], table['cdcLogic'], table['primaryKey'])

    elif data_count >= 1000000:
        
        catalog_schema = ".".join(table['targetTablename'].split(".")[0:-1]) + "."
        intermediate_table = catalog_schema + "intermediate_table"
        windowSpec = Window.partitionBy("partition").orderBy(table['cdcConfiguration']['ColumnName'])
        source_data_df = source_data_df.withColumn("partition", lit(1))
        source_data_df = source_data_df.withColumn("batchno", ntile(10).over(windowSpec))
        batchno_max_value = source_data_df.agg({"batchno": "max"}).collect()[0][0]

        print("Intermediate table start", datetime.now())
        source_data_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable(intermediate_table)
        print("Intermediate table end", datetime.now())

        for i in range(1, batchno_max_value+1):
            temp_df = spark.read.table(intermediate_table)
            temp_df = temp_df.where(temp_df.batchno == i).drop("partition", "batchno")

            print("batch " , i, "started with ", temp_df.count())
            if table['DetokenizationToeknizationFlag'] == 'Y':
                tokenized_df = tokenize_columns(spark, temp_df, table['Tokenizationdetails'])
            else:
                tokenized_df = temp_df
            if table['native_encryption_flag'] == 'Y':
                tokenized_df= column_native_encrypt(tokenized_df, table['native_encryption_columns'])
            
            print("writing batch" , i)

            if table['cdcLogic'] == "overwrite":
                if i == 1:
                    write_data(spark, tokenized_df, table['targetTablename'], table['cdcLogic'], table['primaryKey'])
                elif i > 1:
                    write_data(spark, tokenized_df, table['targetTablename'], "append", table['primaryKey'])
            
            elif table['cdcLogic'] == "merge":
                write_data(spark, tokenized_df, table['targetTablename'], table['cdcLogic'], table['primaryKey'])
            
            elif table['cdcLogic'] == "append":
                write_data(spark, tokenized_df, table['targetTablename'], table['cdcLogic'], table['primaryKey'])

        drop_query = "DROP TABLE " + intermediate_table
        spark.sql(drop_query)

# This function reads data from jdbc table based on cdcConfiguration and tableProperties
def load_data(spark, table, url, jdbc_driver_value, username, password):
    source_data_df = read_jdbc_table_by_table_type(spark, table['SourceTableName'].split(".")[-1], url, jdbc_driver_value, username, password, table['primaryKey'], table['partitionColumns'][0] ,table['SourceTableName'].split(".")[-1], table['targetTablename'], table['cdcLogic'], table['loadType'], table['cdcConfiguration']['ColumnName'], table['cdcConfiguration']['cdcCondition'], table['tableProperties']['tablesize'], table['tableProperties']['fetchsize'], table['cdcConfiguration']['Delta']) 

    tokenize_and_write_data(spark, source_data_df, table)

# 2.14. Module to load history data

# Split_load is used to load the historical data based on recurrence (ie) Monthly, Quarterly or Yearly
def split_load(spark, table, cdc_column, url, driver, username, password, recurrence):
    if spark.catalog.tableExists(table['targetTablename']):
        truncate_query = 'TRUNCATE TABLE ' + table['targetTablename']
        spark.sql(truncate_query)

    df = get_df_by_key(spark, url, table['SourceTableName'], username, password, driver, table['tableProperties']['fetchsize'])
    min_timestamp = df.select(min(cdc_column)).collect()[0][0]
    max_timestamp = df.select(max(cdc_column)).collect()[0][0]

    if recurrence == "monthly":
        diff = (max_timestamp.year - min_timestamp.year) * 12 + max_timestamp.month - min_timestamp.month
    elif recurrence == "quarterly":
        result = (max_timestamp.year - min_timestamp.year) * 12 + max_timestamp.month - min_timestamp.month
        diff = result // 3
    elif recurrence == "yearly":
        diff = max_timestamp.year - min_timestamp.year

    for i in range(0, diff):
        if recurrence == monthly:
            lower = min_timestamp + relativedelta(months=i)
            upper = min_timestamp + relativedelta(months=i+1)
        elif recurrence == quarterly:
            lower = min_timestamp + relativedelta(months=i*3 )
            upper = min_timestamp + relativedelta(months=(i*3)+3)
        elif recurrence == yearly:
            lower = min_timestamp + relativedelta(years=i )
            upper = min_timestamp + relativedelta(years=i+1)

        query = "(select * from " + table['SourceTableName'] + " where " + cdc_column + ">= to_date('" + str(lower) + "', 'YYYY-MM-DD HH24:MI:SS')  and " + cdc_column + "< to_date('" + str(upper) + "', 'YYYY-MM-DD HH24:MI:SS'))"
        if i == diff - 1:
            query = "select * from " + table['SourceTableName'] + " where " + cdc_column + ">= to_date('" + str(lower) + "', 'YYYY-MM-DD HH24:MI:SS')"

        source_data_df = get_df_by_key(spark, url, query, username, password, driver, table['tableProperties']['fetchsize'])

        tokenize_and_write_data(spark, source_data_df, table)
