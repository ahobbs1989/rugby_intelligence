import os

## Website ##
#############
TARGET_SITE = ""


##  Config ##
#############
CHROME_DRIVER_PATH = ""
SELENIUM_SLEEP = 3


## Lakehouse Directories ##
###########################
ROOT_PATH = ""
LAKEHOUSE_PATH = os.path.join(ROOT_PATH, 'lakehouse')

DATA_QUALITY_PATH = os.path.join(LAKEHOUSE_PATH, 'data_quality')



RAW_PATH = os.path.join(LAKEHOUSE_PATH, 'raw')
RAW_GAME_PATH = os.path.join(RAW_PATH, 'game')
RAW_GAME_STAT_PATH = os.path.join(RAW_PATH, 'game_stat')


BRONZE_PATH = os.path.join(LAKEHOUSE_PATH, 'bronze')
BRONZE_TEAM_PATH = os.path.join(BRONZE_PATH, 'team')
BRONZE_GAME_PATH = os.path.join(BRONZE_PATH, 'game')
BRONZE_GAME_STAT_PATH = os.path.join(BRONZE_PATH, 'game_stat')


SILVER_PATH = os.path.join(LAKEHOUSE_PATH, 'silver')
SILVER_GAME_PATH = os.path.join(SILVER_PATH, 'game')
SILVER_GAME_STAT_PATH = os.path.join(SILVER_PATH, 'game_stat')

## Rugby Parameters ##
######################
TEAM_LIST = ['ireland', 'italy', 'argentina', 'south-africa', 'new-zealand',
             'france', 'japan', 'fiji', 'portugal', 'georgia', 'wales', 'scotland', 'australia']


PLAYER_STATS = ["carries", "clean_breaks", "completed_tackles", "turnovers_conceded", "turnovers_won", "carries", "total_kicks", "passes",
                "carries_metres", "clean_breaks", "offloads", "defenders_beaten", "try_assists", "tries", "turnovers_conceded", "carries_per_minute",
                "total_tackles", "missed_tackles", "completed_tackles", "dominant_tackles", "turnovers_won", "ruck_turnovers", "lineouts_won",
                "tackles_per_minute", "red_cards", "yellow_cards", "penalties_conceded"]
