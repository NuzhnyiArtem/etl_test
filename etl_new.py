from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import date, datetime, timedelta

default_args = {
    'mysql_conn_id': 'demo_local_mysql'
}

with DAG('etl_new2023',
         schedule=None,
         default_args=default_args,
         start_date=datetime(2023, 6, 15),
         catchup=False) as dag:
    url_pg = "jdbc:postgresql://127.0.0.1:5432/test1"
    mode = "overwrite"
    properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"}
    def extract(cd, ed):
        """Загружает данные из Mysql таблицы,
         и обрабатывает их разбивая на 3 датафрейма по регионам"""
        conf = SparkConf()
        conf.set("spark.jars", "mysql-connector-j-8.0.33.jar, postgresql-42.6.0.jar")
        spark = SparkSession.builder.appName("ETL").config(conf=conf).getOrCreate()
        jdbc_mysql_url = "jdbc:mysql://127.0.0.1:3306/new_schema"
        jdbc_mysql_driver = "com.mysql.cj.jdbc.Driver"
        while cd <= ed:
            next_date = cd + timedelta(days=30)
            query = f"(SELECT * FROM test WHERE DATE >= '{cd}' and date < '{next_date}') as my_table"

            df = spark.read.format("jdbc").options(
                url=jdbc_mysql_url,
                dbtable="test",
                table=query,
                user="root",
                driver=jdbc_mysql_driver,
                password="!Asdqwe3827155").load()
            cd = next_date
        df.createOrReplaceTempView('df')
        msk = spark.sql(
            'select current_date as date, id, sum(expence) as sum from df where area = "msk" group by id order by id')
        spb = spark.sql(
            'select current_date() as date, id, sum(expence) from df where area = "spb" group by id order by id')
        nn = spark.sql(
            'select current_date() as date, id, sum(expence) from df where area = "nn" group by id order by id')
        return [msk, spb, nn]


    start_date = date(2023, 1, 1)
    end_date = date.today()
    current_date = start_date

    dbases = extract(current_date, end_date, start_date)
    connections = ["msk", "spb", "nn"]

    def load(db, conn):
        db.write.jdbc(url_pg, conn, mode, properties)


    for i in range(len(dbases)):
        """Цикл проходит по списку датафреймов и по списку таблиц в postgresql,
         после чего загружает данные в нужную таблицу"""
        task = PythonOperator(task_id=f"{connections[i]}",
                            python_callable=load,
                            op_kwargs={"db": dbases[i],
                                       "conn": connections[i]},
                            dag=dag)


    def update_high_watermark(**kwargs):
        """Передает данные о выполненной задаче в xcom"""
        current_dt = datetime.utcnow()
        kwargs['ti'].xcom_push(key="high_watermark", value=current_dt)

    high_watermark_task = PythonOperator(
        task_id='print_context_task',
        provide_context=True,
        python_callable=update_high_watermark,
        dag=dag,
        )
    task >> high_watermark_task

