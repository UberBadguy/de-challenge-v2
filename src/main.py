import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, desc, sum, when, input_file_name, udf
from schemas.epl_data_schema import EPLProperties
from util.udf import UDFS
from util.constants import Constants

print('Job Triggered')

# Context Setup
spark = SparkSession.builder.appName('DE Challenge Mario Leon').getOrCreate()

# Read the data
matchesDF = spark.read.schema(EPLProperties.schema).json(Constants.INPUT_FOLDER)

# Define UDF to get season value from filename
getSeasonFromFilenameUDF = udf(UDFS.getSeasonFromFilename)

# Dataframe with results of wins-draws from Home team
homeDf = matchesDF\
    .withColumn('season', getSeasonFromFilenameUDF(input_file_name()))\
    .withColumnRenamed('HomeTeam', 'team')\
    .withColumn('points', when(col('FTR')=='H', lit(3)).when(col('FTR')=='D', lit(1)))\
    .withColumnRenamed('HS', 'goalShots')\
    .withColumnRenamed('FTHG', 'goalsScored')\
    .withColumnRenamed('FTAG', 'goalsReceived')\
    .where(col('FTR').isin('H', 'D'))\
    .select('season', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')

# Dataframe with results of wins-draws from Away team
awayDf = matchesDF\
    .withColumn('season', getSeasonFromFilenameUDF(input_file_name()))\
    .withColumnRenamed('HomeTeam', 'team')\
    .withColumn('points', when(col('FTR')=='A', lit(3)).when(col('FTR')=='D', lit(1)))\
    .withColumnRenamed('HS', 'goalShots')\
    .withColumnRenamed('FTAG', 'goalsScored')\
    .withColumnRenamed('FTHG', 'goalsReceived')\
    .where(col('FTR').isin('A', 'D'))\
    .select('season', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')


# Merge both results
allPointsDF = homeDf.union(awayDf)

# Calculate aggregated values
teamPerformanceDF = allPointsDF.groupBy('season','team').agg(
    sum('points').alias('sum_points'),
    (sum('goalShots')/sum('goalsScored')).alias('shot_goal_ratio'),
    sum('goalsScored').alias('sum_goals_scored'),
    sum('goalsReceived').alias('sum_goals_received'),
    ).select('season', 'team', 'sum_points', 'shot_goal_ratio', 'sum_goals_scored', 'sum_goals_received')

# Define ranking windows partitioned by season
rankPoints = Window.partitionBy('season').orderBy(desc("sum_points"))
rankShotGoalRatio = Window.partitionBy('season').orderBy(desc("shot_goal_ratio"))
rankGoalScored = Window.partitionBy('season').orderBy(desc("sum_goals_scored"))
rankGoalReceived = Window.partitionBy('season').orderBy(desc("sum_goals_received"))

# Apply rankings to team performances dataframe
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

# Write procedure with season as partition value
output_path = f"{Constants.OUTPUT_FOLDER}/epl_results"
rankedResultsDF.write.mode('overwrite').partitionBy('season').json(output_path)

spark.stop()
print('Spark Session Terminated')
