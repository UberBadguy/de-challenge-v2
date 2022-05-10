import os
from pyspark.sql import SparkSession

import os

print('Job Triggered')

# Basic IO definitions - dir_path defaults for local testing
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
input_folder = os.getenv('INPUT_FOLDER', f'{dir_path}\data')
output_folder = os.getenv('OUTPUT_FOLDER', f'{dir_path}\output')

# Context Setup
spark = SparkSession.builder.appName('DE Challenge Mario Leon').getOrCreate()




spark.stop()
print('Spark Session Terminated')
