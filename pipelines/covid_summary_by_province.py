import os
from pyparsing import col
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper.utils import get_spark_app_config
from helper.logger import Log4J

if __name__ == "__main__":
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .config('spark.driver.extraClassPath', os.path.join(project_dir, 'libs/mysql-connector-java-8.0.11.jar')) \
        .getOrCreate()

    logger = Log4J(spark)

    # -----------------> Extract
    case_df = spark \
        .read \
        .csv(os.path.join(project_dir, "data/Indonesia_coronavirus_daily_data.csv"), header=True, inferSchema=True)
    # case_df.printSchema()
    # case_df.show()
    zone_df = spark \
        .read \
        .csv(os.path.join(project_dir, "data/zone_reference.csv"), header=True, inferSchema=True)
    # zone_df.printSchema()
    # zone_df.show()

    # -----------------> Transform
    grouped_case_df = case_df \
        .filter(to_date(col('Date'), 'dd/mm/yyyy') >= datetime.strptime('01-01-2021', '%d-%m-%Y')) \
        .groupby('Province') \
        .agg(sum('Daily_Case').alias("Total_Case"))
    # grouped_case_df.show()

    summary_df = grouped_case_df \
        .join(zone_df,
            (grouped_case_df['Total_Case'] >= zone_df['Min_Total_case']) &
            (grouped_case_df['Total_Case'] <= zone_df['Max_Total_Case']),
            how='inner'
        ).select('Province', 'Total_Case', 'Zone')
    # summary_df.show()

    # -----------------> Load
    partition_date = datetime.today().strftime("%Y%m%d")
    summary_df \
        .repartition(1) \
        .write \
        .csv(os.path.join(project_dir, f"output/summary_by_province_{partition_date}"), mode='overwrite', header=True)

    db_conn: str = "jdbc:mysql://localhost:3306/spark_covid?serverTimezone=Asia/Jakarta&useSSL=false"
    table_name: str = "summary_by_province"
    properties: dict = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        summary_df \
            .repartition(1) \
            .write \
            .mode("overwrite") \
            .jdbc(db_conn, table=table_name, mode='overwrite', properties=properties)
        logger.info('Data has been loaded to MySQL!')
    except Exception as ex:
        logger.error(f'Failed to load data to MySQL : {ex}')
        raise RuntimeError("Failed to load data to MySQL") from ex