import boto3
from datetime import datetime
import configparser
import builtins
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
import boto3
from pyspark.sql.types import StringType, StructField, StructType, DateType, LongType, TimestampType, DoubleType
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
import sys
from pyspark.context import SparkContext
import boto3
from pyspark import SparkConf
from botocore.exceptions import ClientError
import sys
from awsglue.utils import getResolvedOptions
import psycopg2
import io

def get_config(conf_path): 
    bucket_name, prefix = split_aws_url(conf_path)
    s3=boto3.client('s3')
    s3.download_file(bucket_name,prefix,'conf.cfg')
    config = configparser.ConfigParser()
    config.read('conf.cfg')
    confs = {}
    for section  in config.sections():
        confs[section] =  dict(config.items(section))
    return confs['conf_dict']

###################################

def create_spark_session(app_name="OLS_replication", external_crossaccount_catalog_id=None, internal_catalog_id=None, extra_configs=None):
    """
    Creates and returns a Spark session with predefined Iceberg and Glue catalog configurations.
    
    :param app_name: Name of the Spark application.
    :param external_crossaccount_catalog_id: ID of the external cross-account Glue catalog.
    :param internal_catalog_id: ID of the internal Glue catalog.
    :param extra_configs: Additional Spark configurations as a dictionary.
    :return: A configured SparkSession instance.
    """
    spark = SparkSession.builder.appName(app_name)
    # Common Iceberg and Glue catalog configurations
    common_catalog_conf = {
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.int_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.int_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.int_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    }
    # Default configurations
    default_conf = {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.execution.arrow.pyspark.enabled": True,
        "spark.sql.catalog.glue_catalog.glue.skip-name-validation": True,
        "spark.sql.catalog.int_catalog.skip-name-validation": True,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    }
    # Cross-account catalog configurations
    crossaccount_conf = {
        "spark.sql.catalog.glue_catalog.glue.id": external_crossaccount_catalog_id,
        "spark.sql.catalog.int_catalog.glue.id": internal_catalog_id
    }
    # Merge configurations
    all_configs = {**default_conf, **common_catalog_conf, **crossaccount_conf}   
    # Apply configurations
    for key, value in all_configs.items():
        if value is not None:  # Ensure we don't set None values
            spark = spark.config(key, value) 
    # Apply extra configurations if provided
    if extra_configs:
        for key, value in extra_configs.items():
            spark = spark.config(key, value)
    return spark.getOrCreate()

def check_table_exists(spark, table_name: str) -> bool:
    """
    Check if a table exists in any catalog using spark.table().
    Parameters:
        table_name (str): Fully qualified table name (e.g., 'catalog.db.table')
    Returns:
        bool: True if the table exists, False otherwise
    """
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False

def write_parquet_with_glue_table_iceberg(spark, df, dst_table_full_name, partition_cols):
    if check_table_exists(spark, dst_table_full_name):
        df.sortWithinPartitions(*partition_cols) \
                  .writeTo(dst_table_full_name) \
                  .tableProperty("format-version", "2") \
                  .partitionedBy(*partition_cols) \
                  .append()
    else:
        df.sortWithinPartitions(*partition_cols) \
                  .writeTo(dst_table_full_name) \
                  .tableProperty("format-version", "2") \
                  .partitionedBy(*partition_cols) \
                  .createOrReplace()

def list_folders(s3_client, bucket_name, prefix):
    continuation_token = None
    while True:
        params = {'Bucket': bucket_name, 'Prefix': prefix, 'Delimiter': '/'}
        if continuation_token:
            params['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**params)
        for content in response.get('CommonPrefixes', []):
            yield content.get('Prefix')
        if not response.get('IsTruncated', False):
            break
        continuation_token = response['NextContinuationToken']

def split_aws_url(url):
    if url.startswith("s3://"):
        _, _, bucket, prefix = url.split("/", 3)
    else:
        bucket, key = url.split("/", 1)
    return bucket, prefix

def get_dir_list(dir):
    bucket_name, prefix = split_aws_url(dir)
    s3 = boto3.client("s3")
    dirs =  [dir.replace(prefix,'')  for dir in list_folders(s3,bucket_name,prefix)] 
    valid_dirs =  []
    for dir in dirs:
        valid_dirs.append(dir)
    return valid_dirs

def load_ice_data(spark,input_url,ice_dest_path,dst_table_full_name,table_name,rd_src_schema,rd_dst_schema,host,port,database,user,password):
    print('Iceberg data load started!!')
    print(input_url)
    src_folders = get_dir_list(input_url)
    limit_dt = datetime.strptime('20250515','%Y%m%d').date()
    print(src_folders)
    src_dt = [datetime.strptime(dir.split('=')[1][:-1], "%Y%m%d").date() for dir in src_folders]
    src_dt = [dt for dt in src_dt if dt > limit_dt]
    print('Source dates: ', src_dt)
    dest_folders = get_dir_list(ice_dest_path)
    dest_dt = [datetime.strptime(dir.split('=')[1][:-1], "%Y-%m-%d").date() for dir in dest_folders]
    to_load_dt = list(set(src_dt)-set(dest_dt))

    to_load_dt = [builtins.max(src_dt)]
    max_source_date = builtins.max(src_dt)
    src_comparision_path = input_url
    to_load_files = [src_comparision_path+'dt='+dt.strftime("%Y%m%d") for dt in to_load_dt]
    latest_folders = ['dt='+dt.strftime("%Y%m%d") for dt in to_load_dt]
    print('to load files: ', to_load_files)
    print('Iceberg data load completed!!')
    incremental_rows = 0
    schema_change_message = ""
    for file in to_load_files:
        df_iceberg = spark.read.parquet(file)
        dt = datetime.strptime(file.split('=')[1], '%Y%m%d')
        dt = dt.date()
        # dt = dt.strftime("%Y%m%d")
        print('loading for : ', dt)
        df_iceberg = df_iceberg.withColumn('dt', lit(dt))
        # Compare schema and add missing columns as Null, only if table exists
        
        if check_table_exists(spark, dst_table_full_name):
            table_schema = [field.name for field in spark.table(dst_table_full_name).schema.fields]
            df_schema = df_iceberg.columns
            missing_cols = set(table_schema) - set(df_schema)
            new_cols = set(df_schema) - set(table_schema)
            print("Missing columns: ", missing_cols)
            print("New columns: ", new_cols)
            if missing_cols:
                schema_change_message += f"Missing columns in source data compared to destination table: {missing_cols}.\n"
            if new_cols:
                schema_change_message += f"New columns in source data not present in destination table: {new_cols}"    
            for col_name in missing_cols:
                df_iceberg = df_iceberg.withColumn(col_name, lit(None))
        df_iceberg.show(5, False)
        df_count = df_iceberg.count()
        incremental_rows += df_count
        print('Total records to load: ', df_count)
        #List columns name
        columns = df_iceberg.columns
        print(columns)
        #write_parquet_with_glue_table_iceberg(spark, df_iceberg, dst_table_full_name, partition_cols=['dt'])
        #load_to_dsdk_redshift(dst_table_full_name,final_redshift_schema,table_name,host,port,database,user,password, dt)
        load_to_dsdk_redshift(rd_src_schema,table_name,rd_dst_schema,host,port,database,user,password,dt)
    print('Iceberg data load completed!!')
    print("to_load_dt",latest_folders)
    return incremental_rows,latest_folders,schema_change_message,max_source_date

def get_email_subject(dataset, refresh_date):
    """
    Returns the email subject for the refresh notification.
    """
    return f"{dataset} refresh – SUCCESS ({refresh_date})"

def get_email_body_html(dataset, datafolder, schema, table_names, row_counts, access_emails, schema_changes):
    """
    Returns the HTML body for the refresh notification email.
    """
    # paths = [p.strip().rstrip('/') for p in path.split(',')]
    # formatted_folders = [f"[{bucket}/{p}/{datafolder}]" for p in paths]
    # folders_display = ', '.join(formatted_folders)
    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #222;">
        <div style="max-width:600px;">
          Hi Team --<br><br>
          Today's refresh for <b>{dataset}</b> completed <span style="color:green;"><b>SUCCESSFULLY</b></span>.<br><br>
          <b>Source latest folders:</b><br>{''.join([f'{tbl}: {", ".join(folders)}<br>' for tbl, folders in datafolder.items()])}<br>
          <b>Destination Details:</b><br>
          <b>Schema:</b> {schema}<br><br>
          <b>Tables:</b> {', '.join(table_names)}<br><br>
          <b>Incremental Row counts:</b><br>{''.join([f'{tbl}: {counts}<br>' for tbl, counts in row_counts.items()])}<br>
          <b>Access provided for:</b> {', '.join(access_emails)}<br><br>
          <b>Schema Changes:</b> {schema_changes}<br><br>
          Thank you,<br>
          Team AUDS
        </div>
      </body>
    </html>"""

def get_email_body_text(dataset, datafolder, schema, table_names, row_counts, access_emails, schema_changes):
    """
    Returns the plain text body for the refresh notification email.
    """
    # paths = [p.strip().rstrip('/') for p in path.split(',')]
    # formatted_folders = [f"[{bucket}/{p}/{datafolder}]" for p in paths]
    # folders_display = ', '.join(formatted_folders)
    return f"""Hi Team --
Today's refresh for {dataset} completed SUCCESSFULLY.
Source latest folders: {datafolder}
Schema: {schema}
Tables: {', '.join(table_names)}
Incremental Row counts: {row_counts}
Access provided for: {', '.join(access_emails)}
Schema Changes: {schema_changes}

Thank you,
Team AUDS
"""

def send_refresh_email(dataset, refresh_date, datafolder, schema, table_names, 
                      row_counts, access_emails, schema_changes,
                      sender_email, to_emails, cc_emails, reply_to_email, source_arn, 
                      aws_region="us-east-1"):
    """
    Send a dataset refresh success email using AWS SES
    """
    subject = get_email_subject(dataset, refresh_date)
    html_body = get_email_body_html(dataset,  datafolder, schema, table_names, row_counts, access_emails, schema_changes)
    text_body = get_email_body_text(dataset, datafolder, schema, table_names, row_counts, access_emails, schema_changes)
    
    try:
        ses = boto3.client("ses", region_name=aws_region)
        response = ses.send_email(
            Source=sender_email,
            SourceArn=source_arn,
            Destination={
                "ToAddresses": to_emails,
                "CcAddresses": cc_emails
            },
            ReplyToAddresses=reply_to_email,
            Message={
                "Subject": {
                    "Data": subject
                },
                "Body": {
                    "Text": {
                        "Data": text_body
                    },
                    "Html": {
                        "Data": html_body
                    }
                }
            }
        )
        print("Email sent successfully! Message ID:", response["MessageId"])
        return response["MessageId"]
    except Exception as e:
        print(f"Error sending email: {e}")
        raise



def main_email(config_path, row_counts, latest_folders_dict, table_names, schema, dataset,
               access_emails, to_emails, cc_emails, schema_change_message_dict, max_source_date):
    print("max_source_date:", max_source_date, type(max_source_date))

    # Format schema changes per table
    schema_changes_formatted = ""
    for tbl in table_names:
        msg = schema_change_message_dict.get(tbl, "")
        if msg:
            schema_changes_formatted += f"{tbl}: Yes<br>Details: {msg}<br>"
        else:
            schema_changes_formatted += f"{tbl}: No<br>"

    # SLA calculation logic
    # Convert exec_date to date object if needed
    # if isinstance(exec_date, str):
    #     exec_dt = datetime.strptime(exec_date, "%Y-%m-%d").date()
    # else:
    #     exec_dt = exec_date
    # if isinstance(max_source_date, str):
    #     max_source_dt = datetime.strptime(max_source_date, "%Y-%m-%d").date()
    # else:
    #     max_source_dt = max_source_date

    # delta = exec_dt - max_source_dt
    # if delta.days < 1:
    #     sla_status = "Met"
    #     duration_hours = delta.total_seconds() / 3600
    #     if duration_hours < 1:
    #         duration_str = f"{int(duration_hours * 60)} minutes"
    #     else:
    #         duration_str = f"{duration_hours:.1f} hours"
    # else:
    #     sla_status = "Not Met"
    #     duration_str = f"{delta.days} days"

    config_data = get_config(config_path)

    sender_email = config_data['sender_email']
    to_emails = to_emails
    cc_emails = cc_emails
    reply_to_email = [email.strip() for email in config_data['reply_to_email'].split(',')]
    source_arn = config_data['source_arn']
    aws_region = config_data['aws_region']

    DATASET_CONFIG = {
        "dataset": dataset,
        "refresh_date": datetime.today().strftime("%Y-%m-%d"),
        "table_names": table_names,
        "datafolder": latest_folders_dict,
        "schema": schema,
        "row_counts": row_counts,
        "access_emails": access_emails.split(','),
        "schema_changes": schema_changes_formatted
    }

    message_id = send_refresh_email(
        **DATASET_CONFIG,
        sender_email=sender_email,
        to_emails=to_emails,
        cc_emails=cc_emails,
        reply_to_email=reply_to_email,
        source_arn=source_arn,
        aws_region=aws_region
    )
    print(f"Email notification sent with ID: {message_id}")



def main():
    args = getResolvedOptions(sys.argv, ["external_crossaccount_catalog_id","internal_catalog_id","table_names","schema","input_file_path","ice_dest_folder","catalog","dataset","access_emails","to_emails","cc_emails","exec_date","config_path","rd_src_schema","rd_dst_schema"])

    # config_path = "s3://ann32auds-aue1-auds-datateam/scripts/test/1pd_ses_common_config.cfg"

    external_crossaccount_catalog_id = args['external_crossaccount_catalog_id'] #"801684805865"
    internal_catalog_id = args['internal_catalog_id'] #"125876603273"
    table_names = args['table_names'] 
    schema = args['schema']
    input_file_path = args['input_file_path']
    ice_dest_folder = args['ice_dest_folder']
    catalog = args['catalog']
    dataset = args['dataset']
    access_emails = args['access_emails']
    to_emails = args['to_emails']
    cc_emails = args['cc_emails']
    config_path = args['config_path']
    rd_src_schema = args['rd_src_schema']
    rd_dst_schema = args['rd_dst_schema']
    
    

    # external_crossaccount_catalog_id = "801684805865"
    # internal_catalog_id = "125876603273"

    spark = create_spark_session(
        external_crossaccount_catalog_id=external_crossaccount_catalog_id,
        internal_catalog_id=internal_catalog_id
    )
    
    #get creds from ssm
    ssm_config = get_ssm_params()
    creds = ssm_config.get("auds_dsdk_connect", {})
    user = creds.get("username", "").replace('"', '')
    password = creds.get("password", "").replace('"', '')
    host = creds.get("host", "").replace('"', '')
    port = creds.get("port", "").replace('"', '')
    database = creds.get("database", "").replace('"', '')

    

    # table_names = "omd_under_armour_1pd_id_mapping_resolved,omd_under_armour_1pd_id_mapping_full,omd_under_armour_1pd_attributes_attribute_pull"
    # schema = "an_auds_39b50272-7248-43c9-96d9-24c68b0da15e_bronze"
    # input_file_path = "s3://ann32auds-aue1-auds-sync-nonpii/incoming"
    # ice_dest_folder = "s3://ann34orad-aue1-cl-557c2aee-2644-11e5-8254-121cf53dff64/39b50272-7248-43c9-96d9-24c68b0da15e/data/auds/bronze"
    # catalog = "glue_catalog"
    # dataset = "Under Armour"
    # access_emails = "user1@omc.com,user2@omc.com"
    # to_emails = "likhith.rajanna@omc.com,likhithcr15@gmail.com"
    # cc_emails = "likhith.rajanna@omc.com,likhithcr15@gmail.com"
    # exec_date = '2025-11-04'

    table_names = table_names.split(',')
    to_emails = to_emails.split(',')
    cc_emails = cc_emails.split(',')


    row_counts = {}
    latest_folders_dict = {}
    schema_change_message_dict = {}
    for table_name in table_names:

        input_url = f"{input_file_path}/{table_name}/"
        ice_dest_path = f"{ice_dest_folder}/{table_name}/data/"
        dst_table_full_name = f"`{catalog}`.`{schema}`.`{table_name}`"
        incremental_rows, latest_folders,schema_change_message,max_source_date = load_ice_data(spark, input_url, ice_dest_path, dst_table_full_name,table_name,rd_src_schema,rd_dst_schema,host,port,database,user,password)
        row_counts[table_name] = incremental_rows
        latest_folders_dict[table_name] = latest_folders
        schema_change_message_dict[table_name] = schema_change_message
        print(f"Total records loaded for {table_name}: ", row_counts)
        print(f"latest folders: ", latest_folders)

    if latest_folders_dict[table_names[0]] and False:
        main_email(config_path,row_counts, latest_folders_dict,table_names,schema,dataset,access_emails,to_emails,cc_emails,schema_change_message_dict,max_source_date)

def get_ssm_params():
    """
    Fetch credentials and config from AWS SSM Parameter Store.
    The parameter value must be in .ini format, e.g.:
    """

    client = boto3.client("ssm", region_name="us-east-1")
    response = client.get_parameter(
        Name="/auds/service",
        WithDecryption=True
    )
    param_buf = io.StringIO(response['Parameter']['Value'])
    parser = configparser.ConfigParser()
    parser.read_file(param_buf)

    config = {}
    for section in parser.sections():
        config[section] = dict(parser.items(section))
    return config
    



def load_to_dsdk_redshift(rd_src_schema,table_name,rd_dst_schema,host,port,database,user,password,date):
    """
    Function to load data to DSDK Redshift
    """ 
    #CREATE TABLE {final_redshift_schema}.{table_iceberg} AS
    #SELECT * FROM {database_iceberg}.{table_iceberg} limit 0;

    query2 = f"""
    INSERT INTO {rd_dst_schema}.{table_name}
    SELECT * FROM {rd_src_schema}.{table_name} 
    WHERE dt = CAST('{date}' AS DATE)
    """

    print("Executing query:", query2)


    # query = f"""
    # SELECT * FROM {rd_src_schema}.{table_name} 
    # WHERE dt = CAST('{date}' AS DATE)
    # """
    query = f"""
    select * from auds_ib_public.omd_under_armour_1pd_attributes_attribute_pull limit 10;
    """
    

    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Execute SQL commands to create tables and copy data
        #1
        cursor.execute(query)

        # Commit the transaction
        conn.commit()
        print(f"successfully executed query: {query}")

    except Exception as e:
        print(f"An error occurred while loading data into Redshift: {e}")
    finally:
        # Close the connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":

    


    main()
