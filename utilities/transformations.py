from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col


def build_fields(_column_names, _array_cols):
    fields = []
    for column_name in _column_names:
        if column_name in _array_cols:
            fields.append(StructField(column_name, ArrayType(StringType()), True))
        else:
            fields.append(StructField(column_name, StringType(), True))
    return fields


class MasterSchema(object):
    """
    MasterSchema class,  Im ommiting the docstrings here due to the number of parameters
    """

    _column_names = ["this", "is", "a", "list", "of", "cols"]
    _array_cols = ['arraycol']

    _schema = StructType(build_fields(_column_names, _array_cols))

    def __init__(self, spark, df, type, **kwargs):
        """

        :param spark:
        :param df:
        :type df: DataFrame
        :param type:
        :param kwargs:
        """
        self.spark = spark
        self.event_df = df
        self.accountId = type
        for target_column, source_column in kwargs.iteritems():
            setattr(self, target_column, source_column)

    def has_column(self, col_name):
        if not str(col_name) in self.event_df.columns:
            return lit(None)
        else:
            return col(col_name)

    def transform_schema(self):
        """
        takes our mapping config file and transforms existing dataframe to masterschema
        NOTE: this includes the old fields but we will deal with that later
        :return:
        """
        transformed_event_df = self.event_df
        for column_name in self._column_names:
            if hasattr(self, column_name):
                transformed_event_df = transformed_event_df.withColumn(column_name,
                                                                       self.has_column(getattr(self, column_name)))
            else:
                transformed_event_df = transformed_event_df.withColumn(column_name,
                                                                       lit(None))

        transformed_event_df = transformed_event_df.select(self._column_names)
        transformed_event_df = self.spark.createDataFrame(transformed_event_df.rdd, self._schema)

        return transformed_event_df

    def get_schema(self):
        """
        apply master event schema to transformed dataframe

        :return: mastered dataframe
        """
        return self._schema
