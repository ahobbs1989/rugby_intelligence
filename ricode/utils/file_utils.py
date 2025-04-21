import os
import re
from datetime import datetime
import glob
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import global_parameters as gp


#Find year from date string utility function
##############################################
def find_year_in_list(dates: list) -> str:
    date_values = [date.text for date in dates]
    date_year_pattern = r"\b(?:19|20)\d{2}\b"
    for date in date_values:
        years = re.findall(date_year_pattern, str(date))
        if len(years) == 0:
            continue
        else:
            return(years[0])
        
        
#Get formatted date and year from string
##############################################
def format_and_return_date (date_str : str) -> list[str, str]:
    if date_str:
        date_obj = datetime.strptime(date_str, '%a %d %b, %Y')
        return[date_obj.strftime('%Y%m%d'), date_obj.strftime('%Y')]


#Create raw folder and write files per year
##############################################
def write_raw_file(root_folder: str, team: str, year: str, file_prefix: str, raw: any, stat :str = None) -> None:
    folder_path = os.path.join(root_folder)
    if stat:
        folder_path = os.path.join(folder_path, 'game_stats', year, file_prefix)
        file_path = f'{file_prefix}_{stat}.txt'
    else:
        folder_path = os.path.join(folder_path, 'games')
        file_path = f'{file_prefix}_{team}_{year}.txt'
    os.makedirs(folder_path, exist_ok=True)
    with open(
        os.path.join(folder_path, file_path),
        "w",
        encoding="utf-8",
    ) as output_file:
        output_file.write(str(raw))


#Get list of stats collected to prevent re-run
##############################################
def no_stats_collected(stat_no:int = None) -> list:
    run_files = []
    for root, dir, files in os.walk(gp.BRONZE_GAME_STAT_PATH):
        if len(files) ==  stat_no:
            file = root.replace(gp.BRONZE_GAME_STAT_PATH, '').replace('rpi_','')
            run_files.append(file.split('\\')[1])
    return(run_files)


#Get list of specific stat collected to prevent re-run (e.g. teams)
###################################################################
def stat_collected(stat: str) -> list:
    run_files = []
    files = glob.glob(f'{gp.BRONZE_GAME_STAT_PATH}\\*\\*\\*{stat}*')
    for file in files:
        file = file.split('\\')[-1].replace(f'_{stat}.txt', '').replace('rpi_','')
        run_files.append(file)
    return(run_files)


#Format CamelCase
#################
def camel_case(str_val: str, split_val: str = '-') -> str:
    output_str = ''
    str_parts = str_val.split(split_val)
    for i, str_part in enumerate(str_parts):
        str_part = f'{str_part[0].upper()}{str_part[1:]}'
        output_str += ' ' + str_part if i != 0 else str_part  
    return(output_str)


