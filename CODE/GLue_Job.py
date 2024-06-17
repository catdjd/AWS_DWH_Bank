
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime,date, timedelta

# THƯ VIỆN MÌNH VIẾT
import pytz
from secret_manager import *
from utility_funcs import *  
from write_log import *

# create spark and glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# read parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME","pDate","secretNameDwh", "projectName"])
job.init(args['JOB_NAME'], args)
jobName = args['JOB_NAME']
jobRunId = args['JOB_RUN_ID']
secretNameDwh = args["secretNameDwh"]
projectName = args["projectName"]

# set timezone
IST = pytz.timezone('Asia/Ho_Chi_Minh')
pDate = args["pDate"] if args["pDate"] != "0" else str(date.today() + timedelta(days=-1)) # Ngày etl_dl
pDateInt = pDate.replace('-','')
startTime = datetime.now(IST)
startDate = datetime.now(IST).strftime('%Y-%m-%d')
jobType ='Batch'

# Read secret
credential = get_credentials(secretNameDwh)

url = credential['url']
user = credential['username']
password = credential['password']
dbname = credential['dbname']
schemaname = credential['schemaName']
catalogdbdwh = credential['catalogdb']


# extract data source
# Data From S3
csv_dict = "Đường dẫn dữ liệu từ S3"
create_tempview_from_csv_in_s3(csv_dict)

# Data From redshift 
#Extract data từ Redshift awm
redshift_dict = {"tên bảng alias để dùng": "f"""select * from {chemaname}.table ",
}
create_tempview_from_redshift_query(redshift_dict, url, user, password)

# transform
etl = spark.sql(f"""
(
câu lệnh sql
)
""")

#DynamicFrame
# # convert dataframe -> dynamicframe
ip_data = DynamicFrame.fromDF(etl, glueContext,'ip_data')

pre_query = """
truncate table {schemaname}."table"
""".format(schemaname = schemaname)
dbtable = """{schemaname}.{"table"}""".format(schemaname=schemaname)

##################$ Load to target $###############################
write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = ip_data, catalog_connection =catalogdbdwh, redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query, "dbtable": dbtable, "database": dbname})

# write log
write_log(glueContext, catalogdbdwh, dbname, 'awm', args["TempDir"],jobName, pDate, startTime, projectName, jobType, jobRunId) 


job.commit()

