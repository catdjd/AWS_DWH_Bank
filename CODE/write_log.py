from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

def write_log(glueContext, catalogdb, dbname, schemaName, redshiftTmpDir,jobName, pDate, startTime, projectName, jobType, jobRunId):
    spark = SparkSession.builder.getOrCreate()
    startDate = startTime.strftime('%Y-%m-%d')
    sql_log = """
        Select '{jobName}' job_nm
               , '{projectName}' prj_nm
               , '{jobType}' job_tpy
               , to_date('{pDate}', 'yyyy-MM-dd') ETL_DT
               , '{jobRunId}' job_run_id
               , cast(1 as integer) STATUS
               , to_date('{startDate}', 'yyyy-MM-dd') strt_dt 
               , FROM_UTC_TIMESTAMP(TIMESTAMP'{startTime}' ,'Asia/Ho_Chi_Minh' ) STRT_TM
               , FROM_UTC_TIMESTAMP(current_timestamp(),'Asia/Ho_Chi_Minh') END_TM
               , 'Glue end job' NOTE
         """.format(jobName=jobName, pDate=pDate, startTime=startTime, projectName=projectName, startDate=startDate,
                    jobType=jobType, jobRunId=jobRunId)
    print(sql_log)

    log_df = spark.sql(sql_log)
# Chuyển thành dataframe
    log_dyf = DynamicFrame.fromDF(log_df, glueContext, "log_dyf")
    
    # Xóa các bản ghi từ bảng ETL_LOG nếu job được chạy lại trong ngày
    pre_query = """ Delete from {schemaName}.ETL_LOG 
    WHERE ETL_DT = to_date('{pDate}', 'yyyy-MM-dd') 
    AND JOB_NM = '{jobName}' """.format(pDate=pDate, jobName=jobName, jobRunId=jobRunId, schemaName=schemaName)
    # Bắt đầu phiên làm việc mới 
    post_query = """ begin transaction;

    DELETE FROM {schemaName}.ETL_LOG_HIST
    WHERE ETL_DT = to_date('{pDate}', 'yyyy-MM-dd') 
    AND JOB_NM = '{jobName}' 
    AND JOB_RUN_ID = '{jobRunId}';

    INSERT INTO {schemaName}.ETL_LOG_HIST (job_nm, prj_nm, job_tpy, etl_dt, job_run_id, status, strt_dt, strt_tm, end_tm, note)
    SELECT job_nm, prj_nm, job_tpy, etl_dt, job_run_id, status, strt_dt, strt_tm, end_tm, note
    FROM {schemaName}.ETL_LOG
    WHERE  ETL_DT = to_date('{pDate}', 'yyyy-MM-dd') 
    And JOB_NM = '{jobName}' 
    And JOB_RUN_ID = '{jobRunId}';

    end transaction;""".format(pDate=pDate, jobName=jobName, jobRunId=jobRunId, schemaName=schemaName)

    dbtable = """{schemaName}.ETL_LOG""".format(schemaName=schemaName)
    # Kết thúc phiên làm việc, ghi dữ liệu vào db trên redshift
    end_etl_log_df = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=log_dyf,
        catalog_connection=catalogdb,
        connection_options={
            "database": dbname,
            "dbtable": dbtable,
            "preactions": pre_query,
            "postactions": post_query
        },
        redshift_tmp_dir=redshiftTmpDir,
        transformation_ctx="end_etl_log_df",
    )



