import logging
import re
import os
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import col, udf, when, lit, split, max as fmax, \
    sum as fsum
from pyspark.sql.types import StructType, StringType, IntegerType, \
    StructField, DateType
from boto.s3.connection import S3Connection
from internal_utilities.app_config import ACCOUNT_ID_TO_CLIENT_TYPE, \
        AWS_BUCKET_NAME, ACCOUNT_ID_TO_CASSANDRA_KEYSPACE_NAME, ACCOUNT_ID_TO_CLIENT_NAME


class DataReader(object):
    """
    Class for reading data.
    """
    def __init__(self, spark, data_type):
        self.spark = spark
        self.data_type = data_type


    def __repr__(self):
        """
        TODO: add more info and params to actually make this useful
        Define a custom method to print object data when the object is printed in console.
        """
        return ('DataReader reading data type: {}.').format(self.data_type)

    def get_latest_s3_directory(self, key):
        """
        get the most recent modified directory on s3
        :param key: path of s3 sans top level bucket
        :return: string
        """
        s3_connection = S3Connection(host="s3.amazonaws.com")
        bucket = s3_connection.get_bucket(AWS_BUCKET_NAME)
        key_list = []

        for key in bucket.list(prefix=key):
            key_list.append(key)

        latest_file = \
        sorted([(datetime.strptime(key.last_modified.split('T')[0], "%Y-%m-%d"), key.name) for key in key_list])[-1][
            1]
        latest_path = latest_file.rsplit('/', 1)
        latest_directory = latest_path[0]
        return latest_directory

    def get_parquet(self, path):
        """

        :param path: can be s3 or local,  requres full path(eg. file:/// or s3://
        :return: dataframe of parquet
        """
        parquet_data = self.spark.read.parquet(path)
        return parquet_data