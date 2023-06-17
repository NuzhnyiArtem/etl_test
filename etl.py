from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import date, datetime, timedelta

default_args = {
    'mysql_conn_id': 'demo_local_mysql'
}

with DAG('etl_test_dag',
         schedule=None,
         default_args=default_args,
         start_date=datetime(2023, 6, 15),
         catchup=False) as dag:




    conf = SparkConf()
    conf.set("spark.jars", "mysql-connector-j-8.0.33.jar, postgresql-42.6.0.jar")
    spark = SparkSession.builder.appName("ETL").config(conf=conf).getOrCreate()
    jdbc_mysql_url = "jdbc:mysql://127.0.0.1:3306/new_schema"
    jdbc_mysql_driver = "com.mysql.cj.jdbc.Driver"
    url_pg = "jdbc:postgresql://127.0.0.1:5432/test1"
    mode = "overwrite"
    properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    start_date = date(2023, 1, 1)
    end_date = date.today()
    current_date = start_date

    while current_date < end_date:
        next_date = current_date + timedelta(days=30)
        query = f"(SELECT * FROM test WHERE DATE >= '{current_date}' and date < '{next_date}') as my_table"

        df = spark.read.format("jdbc").options(
            url=jdbc_mysql_url,
            dbtable="test",
            table=query,
            user="root",
            driver=jdbc_mysql_driver,
            password="!Asdqwe3827155").load()
        current_date = next_date
    df.createOrReplaceTempView('df')
    msk = spark.sql(
        'select current_date as date, id, sum(expence) as sum from df where area = "msk" group by id order by id')
    spb = spark.sql(
        'select current_date() as date, id, sum(expence) from df where area = "spb" group by id order by id')
    nn = spark.sql(
        'select current_date() as date, id, sum(expence) from df where area = "nn" group by id order by id')

    dbases = [msk, spb, nn]
    conn = ["msk", "spb", "nn"]

    def load(db, conn):
        db.write.jdbc(url_pg, conn, mode, properties)


    for i in range(len(dbases)):
        task = PythonOperator(task_id=f"{conn[i]}",
                            python_callable=load,
                            op_kwargs={"db": dbases[i],
                                       "conn": conn[i]},
                            dag=dag)

        task

