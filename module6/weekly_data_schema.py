from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType

weekly_data_schema = StructType([StructField('year', IntegerType(), True),
            StructField('timeframe', StringType(), True),
            StructField('week', TimestampType(), True),
            StructField('counts_31_counters', IntegerType(), True),
            StructField('covid_period', StringType(), True),
            StructField('pedestrians_14_counters', IntegerType(), True),
            StructField('bikes_14_counters', IntegerType(), True)])
                          
weekly_data_datetime_format = 'M-d-yyyy'