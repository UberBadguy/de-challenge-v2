import os

class Constants():
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    INPUT_FOLDER = os.getenv('INPUT_FOLDER', f'{dir_path}\data')
    OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', f'{dir_path}\output')
