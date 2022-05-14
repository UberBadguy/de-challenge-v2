from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

def rank_teams_seasons_by(criteria):
    return row_number().over(Window.partitionBy('season').orderBy(desc(criteria)))