import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, desc, sum, when, input_file_name, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


print('Job Triggered')

# Basic IO definitions - dir_path defaults for local testing
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
input_folder = os.getenv('INPUT_FOLDER', f'{dir_path}\data')
output_folder = os.getenv('OUTPUT_FOLDER', f'{dir_path}\output')

# Context Setup
spark = SparkSession.builder.appName('DE Challenge Mario Leon').getOrCreate()

# input schema definition - basic data for job
inputSchema = StructType([
      StructField("HomeTeam",StringType(),False),
      StructField("AwayTeam",StringType(),False),
      StructField("Date",StringType(),False),
      StructField("HS",IntegerType(),False),
      StructField("AS",IntegerType(),False),
      StructField("FTHG",IntegerType(),False),
      StructField("FTAG",IntegerType(),False),
      StructField("FTR",StringType(),False)
  ])


def season(x):
    index = x.rindex('/') 
    outsubstr = x[index+1:]
    return outsubstr.replace('season-','').replace('_json.json','')

seasonUDF = udf(lambda z: season(z))

matchesDF = spark.read.schema(inputSchema).json(f"{input_folder}/")

# by home results
homeDf = matchesDF\
    .withColumn('season', seasonUDF(input_file_name()))\
    .withColumn('team', col('HomeTeam'))\
    .withColumn('date', col('Date'))\
    .withColumn('points', when(col('FTR')=='H', lit(3)).when(col('FTR')=='D', lit(1)))\
    .withColumn('goalShots', col('HS'))\
    .withColumn('goalsScored', col('FTHG'))\
    .withColumn('goalsReceived', col('FTAG'))\
    .where(col('FTR').isin('H', 'D'))\
    .select('season', 'date', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')

# by away results
awayDf = matchesDF\
    .withColumn('season', seasonUDF(input_file_name()))\
    .withColumn('team', col('AwayTeam'))\
    .withColumn('date', col('Date'))\
    .withColumn('points', when(col('FTR')=='A', lit(3)).when(col('FTR')=='D', lit(1)))\
    .withColumn('goalShots', col('AS'))\
    .withColumn('goalsScored', col('FTAG'))\
    .withColumn('goalsReceived', col('FTHG'))\
    .where(col('FTR').isin('A', 'D'))\
    .select('season', 'date', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')

allPointsDF = homeDf.union(awayDf)

teamPerformanceDF = allPointsDF.groupBy('season','team').agg(
    sum('points').alias('sum_points'),
    (sum('goalShots')/sum('goalsScored')).alias('shot_goal_ratio'),
    sum('goalsScored').alias('sum_goals_scored'),
    sum('goalsReceived').alias('sum_goals_received'),
    ).select('season', 'team', 'sum_points', 'shot_goal_ratio', 'sum_goals_scored', 'sum_goals_received')

rankPoints = Window.partitionBy('season').orderBy(desc("sum_points"))
rankShotGoalRatio = Window.partitionBy('season').orderBy(desc("shot_goal_ratio"))
rankGoalScored = Window.partitionBy('season').orderBy(desc("sum_goals_scored"))
rankGoalReceived = Window.partitionBy('season').orderBy(desc("sum_goals_received"))

rankedResultsDF = teamPerformanceDF\
    .withColumn('pointRank', row_number().over(rankPoints))\
    .withColumn('shotGoalRatioRank', row_number().over(rankShotGoalRatio))\
    .withColumn('goalsScoredRank', row_number().over(rankGoalScored))\
    .withColumn('goalsReceivedRank', row_number().over(rankGoalReceived))\
    .select(
        'season',
        'team',
        'sum_points',
        'pointRank',
        'shot_goal_ratio',
        'shotGoalRatioRank',
        'sum_goals_scored',
        'goalsScoredRank',
        'sum_goals_received',
        'goalsReceivedRank'
    )

rankedResultsDF.write.mode('overwrite').partitionBy('season').json(f"{output_folder}/results")

spark.stop()
print('Spark Session Terminated')
