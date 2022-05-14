import os

DIR_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
INPUT_FOLDER = os.getenv('INPUT_FOLDER', f'{DIR_PATH}\data')
OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', f'{DIR_PATH}\output\epl_results')

MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%-y/%m/%d %H:%M:%S"