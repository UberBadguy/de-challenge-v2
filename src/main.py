from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, when, input_file_name
from schemas.epl_data_schema import EPL_SCHEMA
from util.constants import INPUT_FOLDER, OUTPUT_FOLDER
from util.udf import get_season_from_filename
from util.functions import rank_teams_seasons_by, format_output_folder
from util.logger_setup import logger


if __name__ == "__main__":

    logger.info('Job Triggered')

    # Context Setup
    spark = SparkSession.builder.appName('DE Challenge v2 Mario Leon').getOrCreate()

    # Read the data
    matchesDF = spark.read.schema(EPL_SCHEMA).json(INPUT_FOLDER)

    # Dataframe with results of wins-draws from Home team
    homeDf = matchesDF\
        .withColumn('season', get_season_from_filename(input_file_name()))\
        .withColumnRenamed('HomeTeam', 'team_name')\
        .withColumn('points', when(col('FTR')=='H', lit(3)).when(col('FTR')=='D', lit(1)))\
        .withColumnRenamed('HS', 'goal_shots')\
        .withColumnRenamed('FTHG', 'goals_scored')\
        .withColumnRenamed('FTAG', 'goals_received')\
        .where(col('FTR').isin('H', 'D'))\
        .select('season', 'team_name', 'points', 'goal_shots', 'goals_scored', 'goals_received')

    # Dataframe with results of wins-draws from Away team
    awayDf = matchesDF\
        .withColumn('season', get_season_from_filename(input_file_name()))\
        .withColumnRenamed('AwayTeam', 'team_name')\
        .withColumn('points', when(col('FTR')=='A', lit(3)).when(col('FTR')=='D', lit(1)))\
        .withColumnRenamed('HS', 'goal_shots')\
        .withColumnRenamed('FTAG', 'goals_scored')\
        .withColumnRenamed('FTHG', 'goals_received')\
        .where(col('FTR').isin('A', 'D'))\
        .select('season', 'team_name', 'points', 'goal_shots', 'goals_scored', 'goals_received')

    # Merge both results
    allPointsDF = homeDf.union(awayDf)

    # Calculate aggregated values
    teamPerformanceDF = allPointsDF.groupBy('season','team_name').agg(
        sum('points').alias('points'),
        (sum('goal_shots')/sum('goals_scored')).alias('shot_goal_ratio'),
        sum('goals_scored').alias('sum_goals_scored'),
        sum('goals_received').alias('sum_goals_received'))

    # Apply rankings to team performances dataframe
    rankedResultsDF = teamPerformanceDF\
        .withColumn('season_rank', rank_teams_seasons_by("points"))\
        .withColumn('shot_goal_ratio_rank', rank_teams_seasons_by("shot_goal_ratio"))\
        .withColumn('goals_scored_rank', rank_teams_seasons_by("sum_goals_scored"))\
        .withColumn('goals_received_rank', rank_teams_seasons_by("sum_goals_received"))

    # Write procedure to csv with header
    rankedResultsDF.coalesce(1).write.mode('overwrite').csv(OUTPUT_FOLDER, header='true')

    spark.stop()
    logger.info('Spark Session Terminated')

    # Format output folder
    format_output_folder()
    logger.info('Output formatted')

    logger.info('Job Finished!')
