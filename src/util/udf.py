from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def filename_cleanup(filename):
    index = filename.rindex('/') 
    outsubstr = filename[index+1:]
    return outsubstr.replace('season-','').replace('_json.json','')

@udf(returnType=StringType()) 
def get_season_from_filename(x):
    return filename_cleanup(x)
