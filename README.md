# NYT Article Data ETL Project

This postgresql project is designed to fetch New York Times article data from [NYT Kaggle Dataset](https://www.kaggle.com/datasets/aryansingh0909/nyt-articles-21m-2000-present), 
clean and process the data, and insert it into a postgresql datalake and data warehouse tables. 
The project is scheduled to run every day at 12am and to retrieve new data from [NYT ARTICLE SEARCH API](https://developer.nytimes.com/docs/articlesearch-product/1/overview).
<br> The project is organized into several directories, each serving a specific purpose.


## Notes:

* The project utilizes Pydantic data classes in the csv_model.py module to clean and process the data. 
* The cleaning process involves adding unique values to the web_url columns in the datalake and data warehouse tables to eliminate duplicate data. <br> 
This is done using Pandas DataFrame, comparing and dropping duplicate web_urls. 
* The csv data is copied to the postgresql table using the COPY command. 
* The project fetches new data from the NYT Article Search API every day. <br>
If there is missing API data, the project retrieves it by obtaining the last date from the database.


## Project Structure:

1. **clean_csv_data**
   - **clean_duplicates.py**: <br>
   Removes duplicate entries from the csv data.
   - **csv_model.py**: <br>
   Defines data classes for cleaning and processing the csv data.
   - **validate_csv.py**: <br>
   Validates the integrity of the csv data.

2. **config**
   - **config.ini**: <br>
   Configuration file for general project settings.
   - **table_config.ini**: <br>
   Configuration file for table-column settings.

3. **get_api_data**
   - **api_data_to_table.py**: <br>
   Inserts data obtained from APIs into the datalake and data warehouse tables.
   - **get_nyt_api.py**: <br>
   Fetches data from the New York Times API.
   
4. **helper**
     - **get_last_date.py**: <br>
   Helper script to get the last date of the data in the datalake table.

5. **move_data**
   - **csv_to_datalake.py**: <br>
   Transfers data from csv to the Postgresql datalake table using copy command.
   - **move_data_to_warehouse.py**: <br>
   Moves data from the datalake table to the data warehouse table.

6. **scripts**
   - **run_once.py**: <br>
   Script for one-time execution tasks.
   - **scheduled_run.py**: <br>
   Script for tasks that need to be executed on a schedule.

7. **utils**
   - **create_postgres_table.py**: <br>
   Creates tables in postgresql..
   - **logger.py**: <br>
   Logging utility.
   - **postgres_connector.py**: <br>
   Creating a connector for postgresql.
   - **read_config.py**: <br>
   Utility for reading a configuration files.
   - **main.py**: <br>
   Main script to run the overall data processing.


### How to use:

* Make sure to install the required modules listed in requirements.txt before running the project.
```
   pip install -r requirements.txt
   ```
* Update the configuration details in config.ini before executing the scripts. 


### API Key Considerations:

To use the New York Times Article Search API, follow these steps:

1. Sign up at the [NYT Dev Portal](https://developer.nytimes.com/accounts/login).
2. Select "My Apps" and add a new app.
3. Add a name for your app, enable the Article Search API and save the app.
