import re
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName('Ops').getOrCreate()

# Get year
extract_year = re.compile(r'./data/MinneMUDAC_raw_files/(\d{4})_metro_tax_parcels.txt')
get_year = lambda path: extract_year.search(path).group(1)

# Make dataframe
make_data_frame = lambda file_path: spark.read.csv(file_path, sep='|', header=True)