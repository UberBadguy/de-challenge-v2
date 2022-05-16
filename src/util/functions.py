import os
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from util.constants import OUTPUT_FOLDER

def rank_teams_seasons_by(criteria):
    return row_number().over(Window.partitionBy('season').orderBy(desc(criteria)))

def format_output_folder(folder=OUTPUT_FOLDER):
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    for file in os.listdir(folder):
        if file.endswith(".csv"):
            os.rename(os.path.join(folder, file), os.path.join(folder, f'epl_results_{timestamp}.csv'))
        else:
            os.remove(os.path.join(folder, file))
