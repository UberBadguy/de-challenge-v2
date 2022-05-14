from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, when, input_file_name
from schemas.epl_data_schema import EPL_SCHEMA
from util.constants import INPUT_FOLDER, OUTPUT_FOLDER
from util.udf import get_season_from_filename
from util.functions import rank_teams_seasons_by
from util.logger_setup import logger


logger.info('Job Triggered')

# Context Setup
spark = SparkSession.builder.appName('DE Challenge Mario Leon').getOrCreate()

# Read the data
matchesDF = spark.read.schema(EPL_SCHEMA).json(INPUT_FOLDER)

# Dataframe with results of wins-draws from Home team
homeDf = matchesDF\
    .withColumn('season', get_season_from_filename(input_file_name()))\
    .withColumnRenamed('HomeTeam', 'team')\
    .withColumn('points', when(col('FTR')=='H', lit(3)).when(col('FTR')=='D', lit(1)))\
    .withColumnRenamed('HS', 'goalShots')\
    .withColumnRenamed('FTHG', 'goalsScored')\
    .withColumnRenamed('FTAG', 'goalsReceived')\
    .where(col('FTR').isin('H', 'D'))\
    .select('season', 'team', 'points', 'goalShots', 'goalsScored', 'goalsReceived')

# Dataframe with results of wins-draws from Away team
awayDf = matchesDF\
    .withColumn('season', get_season_from_filename(input_file_name()))\
    .withColumnRenamed('AwayTeam', 'team')\
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
    sum('goalsReceived').alias('sum_goals_received'))

# Apply rankings to team performances dataframe
rankedResultsDF = teamPerformanceDF\
    .withColumn('pointRank', rank_teams_seasons_by("sum_points"))\
    .withColumn('shotGoalRatioRank', rank_teams_seasons_by("shot_goal_ratio"))\
    .withColumn('goalsScoredRank', rank_teams_seasons_by("sum_goals_scored"))\
    .withColumn('goalsReceivedRank', rank_teams_seasons_by("sum_goals_received"))

# Write procedure with season as partition value
rankedResultsDF.write.mode('overwrite').partitionBy('season').json(OUTPUT_FOLDER)

spark.stop()
logger.info('Spark Session Terminated')
