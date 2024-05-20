import datetime
import logging

from pyspark.sql import SparkSession

from airflow.core.conf_base import ConfBase
# from crate import client as crate_client

class EtlBase(object):
    def __init__(self, spark, conf):
        assert isinstance(spark, SparkSession)
        assert isinstance(conf, ConfBase)
        self.spark = spark
        self.conf = conf
        self.info_log = "sourceID='{0}' filename='{1}' ds='{2}'".format(conf.source_id, conf.in_file, conf.ds)
        self.df = None
        logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(filename)s - Line: %(lineno)d - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def before_read(self):
        pass

    def read_file(self):
        raise Exception("abstract, implement")


    def etl_process(self):
        raise Exception("abstract, implement")

    def before_export(self):
        pass

    def export(self):
        raise Exception("abstract, implement")

    def pipeline(self):
        logger = logging.getLogger()
        app_id = self.spark.sparkContext._jsc.sc().applicationId()

        beg_time = datetime.datetime.now()
        # ds = self.conf.ds
        info_log = self.info_log
        logger.info(info_log)

        logger.info("{0} step='read'".format(info_log))
        self.before_read()
        self.read_file()
        logger.info("{0} step='etl'".format(info_log))
        # if len(self.df.columns) < 2:
        #     print("empty")
        #     if self.conf.skip_empty:
        #         exit(0)
        #     exit(1)
        if self.df is None:
            logger.info("empty")
            if self.conf.skip_empty:
                exit(0)
            exit(1)
        self.etl_process()
        logger.info("{0} step='export'".format(info_log))
        self.before_export()
        self.export()
        end_time = datetime.datetime.now()
        duration = end_time - beg_time
        logger.info("{0} step='done' duration='{1}'".format(info_log, duration))

    def exec_sql_cratedb(self, sql):
        crate_py_url = self.conf.crate_py_url
        with crate_client.connect(crate_py_url, timeout=5, error_trace=True) as con_crate:
            cursor = con_crate.cursor()
            cursor.execute(sql)
            con_crate.commit()
            cursor.close()

    def exec_sql_get_values(self, sql):
        crate_py_url = self.conf.crate_py_url
        rs = list()
        with crate_client.connect(crate_py_url, timeout=5, error_trace=True) as con_crate:
            cursor = con_crate.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                rs.append(row)
            cursor.close()
        return rs

    def exec_info_cratedb(self):
        crate_py_url = self.conf.crate_py_url
        crate_user = self.conf.crate_user
        crate_pass = self.conf.crate_pass
        with crate_client.connect(crate_py_url, timeout=5, error_trace=True,
                                  username=crate_user, password=crate_pass) as con_crate:
            cursor = con_crate.cursor()
            cursor.execute("SELECT table_schema, table_name FROM information_schema.tables")
            for row in cursor.fetchall():
                print(row)
            cursor.close()

    def insert_cratedb(self, sql_drop=None):
        jdbc_url = self.conf.crate_jdbc_url
        jdbc_driver = self.conf.driver_crate_class
        df = self.df
        ds = self.conf.ds
        tbl_name = "{0}.{1}".format(self.conf.source_id.lower(), self.conf.in_file.lower())

        # drop current partition
        if sql_drop is None:
            sql_drop = "DELETE FROM {0} WHERE ds = '{1}' AND " \
                       "ts=date_trunc('week', '+07:00', date_format('%Y-%m-%d','+07:00','{1}'))".format(tbl_name, ds)
        sql_check_data = "SELECT * FROM {0} LIMIT 1".format(tbl_name)
        print(sql_check_data)
        data = self.exec_sql_get_values(sql_check_data)
        # print(data)
        if data:
            print(sql_drop)
            self.exec_sql_cratedb(sql_drop)

        df.na.fill('null').repartition(10).write.mode(
            "append"
        ).option(
            "driver", jdbc_driver
        ).jdbc(
            jdbc_url, tbl_name
        )

    def insert_hive(self, sql_drop=None):
        df = self.df
        spark = self.spark
        ds = self.conf.ds
        tbl_name = "{0}.{1}".format(self.conf.source_id.lower(), self.conf.in_file.lower())
    
        if sql_drop is None:    
            # drop current ds
            sql_drop = "ALTER TABLE {0} DROP IF EXISTS PARTITION (ds='{1}')".format(tbl_name, ds)
        print(sql_drop)
        spark.sql(sql_drop)
        db_cols = spark.sql("SELECT * FROM {0} LIMIT 1".format(tbl_name)).columns
        num_out_partition = self.conf.num_out_partition
        df.select(db_cols).repartition(num_out_partition).write.insertInto(tbl_name)