import os
from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, desc, sum, when

import os

print('Job Triggered')

# Basic IO definitions - dir_path defaults for local testing
dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
input_folder = os.getenv('INPUT_FOLDER', f'{dir_path}\data')
output_folder = os.getenv('OUTPUT_FOLDER', f'{dir_path}\output')

source_files = [f for f in listdir(input_folder) if isfile(join(input_folder, f))]

# Context Setup
spark = SparkSession.builder.appName('DE Challenge Mario Leon').getOrCreate()

# Data Read
for season_json in source_files:
    
    val = season_json.replace('season-','').replace('_json.json','')
    matchesDF = spark.read.json(f"{input_folder}/{season_json}")

    # by home results
    homeDf = matchesDF\
        .withColumn('team', col('HomeTeam'))\
        .withColumn('date', col('Date'))\
        .withColumn('points', when(col('FTR')=='H', lit(3)).when(col('FTR')=='D', lit(1)))\
        .withColumn('goalShots', col('HS'))\
        .withColumn('goalsScored', col('FTHG'))\
        .withColumn('goalsReceived', col('FTAG'))\
        .where(col('FTR').isin('H', 'D'))\
        .select('date', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')

    # by away results
    awayDf = matchesDF\
        .withColumn('team', col('AwayTeam'))\
        .withColumn('date', col('Date'))\
        .withColumn('points', when(col('FTR')=='A', lit(3)).when(col('FTR')=='D', lit(1)))\
        .withColumn('goalShots', col('AS'))\
        .withColumn('goalsScored', col('FTAG'))\
        .withColumn('goalsReceived', col('FTHG'))\
        .where(col('FTR').isin('A', 'D'))\
        .select('date', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')

    allPointsDF = homeDf.union(awayDf)

    teamPerformanceDF = allPointsDF.groupBy('team').agg(
        sum('points').alias('sum_points'),
        (sum('goalShots')/sum('goalsScored')).alias('shot_goal_ratio'),
        sum('goalsScored').alias('sum_goals_scored'),
        sum('goalsReceived').alias('sum_goals_received'),
        ).select('team', 'sum_points', 'shot_goal_ratio', 'sum_goals_scored', 'sum_goals_received')

    rankPoints = Window.orderBy(desc("sum_points"))
    rankShotGoalRatio = Window.orderBy(desc("shot_goal_ratio"))
    rankGoalScored = Window.orderBy(desc("sum_goals_scored"))
    rankGoalReceived = Window.orderBy(desc("sum_goals_received"))

    rankedResultsDF = teamPerformanceDF\
        .withColumn('season', lit(val))\
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
