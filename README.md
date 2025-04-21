# Rugby Intelligence Data Pipeline

This project is a data pipeline for processing international rugby match data. It extracts, transforms, and analyzes data from raw website sources to generate insights about rugby matches, teams, and statistics. 
The pipeline is divided into multiple stages: Web to Raw, Raw to Bronze, Bronze to Silver, Data Analysis and Data Quality.

------------------------------------------------------------------------------

## Project Structure

The project is organized as follows:



ricode/ 


      ├── web_to_raw/ 
            ├── international_game_scrape.py
            ├── international_game_stats_scrape.py
            ├── international_game_team_scrap.py
            ├── orchestrator.py

            
      ├── raw_to_bronze/
            ├── rtb_game.py 
            ├── rtb_game_stat.py 
            ├── rtb_team.py         

            
      ├── bronze_to_silver/ 
            ├── bts_game.py 
            ├── bts_game_stat.py 

            
      ├── data_quality/  
            ├── dq_raw_to_bronze.py
            ├── dq_bronze_to_silver.py

            
      ├── data_analysis/  
            ├── game_stat_analysis_demo.ipynb

            
      ├── utils/ 
            ├── file_utils.py

            
       ├── test/ 
           ├── test_file_utils.py

------------------------------------------------------------------------------

## Pipeline Stages

### 1. **Web to Raw**
   - **Purpose**: Scrapes rugby match data from web sources and saves it in raw format.
   - **Key File**: `orchestrator.py`
   - **Functions**:
     - Scrapes match data, including teams, scores, and stats.
     - Saves raw HTML files for further processing.

### 2. **Raw to Bronze**
   - **Purpose**: Processes raw data into a structured format (Bronze layer).
   - **Key Files**:
     - `rtb_game.py`: Processes game-level data.
     - `rtb_game_stat.py`: Processes game statistics.
     - `rtb_team.py`: Processes team-level data.
   - **Functions**:
     - Extracts structured data from raw HTML files.
     - Saves data in Delta Lake format.

### 3. **Bronze to Silver**
   - **Purpose**: Enhances and cleans Bronze data for analysis (Silver layer).
   - **Key Files**:
     - `bts_game.py`: Processes game-level data.
     - `bts_game_stat.py`: Processes game statistics.
   - **Functions**:
     - Cleans and enriches data.
     - Combines multiple datasets for analysis.

### 4. **Data Quality**
   - **Purpose**: Ensures data quality at each stage of the pipeline.
   - **Key Files**:
     - `dq_raw_to_bronze.py`: Validates data from Raw to Bronze.
     - `dq_bronze_to_silver.py`: Validates data from Bronze to Silver.
   - **Functions**:
     - Checks for missing values, duplicates, and invalid data.
     - Generates data quality reports.

### 5. **Data Analysis**
   - **Purpose**: Analyzes rugby match data to generate insights.
   - **Key File**: `game_stat_analysis_demo.ipynb`
   - **Functions**:
     - Visualizes team performance and match statistics.
     - Correlates key metrics like tackles, possession, and territory.

------------------------------------------------------------------------------

## Key Utilities

### `file_utils.py`
   - Provides helper functions for file operations:
     - `write_raw_file`: Writes raw data to files.
     - `find_year_in_list`: Extracts the year from a list of dates.
     - `format_and_return_date`: Formats dates into specific formats.
     - `no_stats_collected`: Checks for missing stats.
     - `stat_collected`: Checks if specific stats have already been collected.

### Dependencies
      Python 3.8+
      PySpark
      Delta Lake
      Matplotlib
      Seaborn
      BeautifulSoup4
