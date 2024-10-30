# Top 10 Largest Banks ETL Process

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline to extract data on the world's largest banks ranked by market capitalization, convert the values into multiple currencies, and store the processed data for analysis. The data is sourced from [this link](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks).

## Technologies Used
- Python
- Libraries: `pandas`, `BeautifulSoup`, `requests`, `sqlite3`, `numpy`, `datetime`

## How It Works
1. **Data Extraction**: 
   - The pipeline begins by extracting data from the [Wikipedia page](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks) that lists the largest banks by market capitalization using web scraping with the `requests` and `BeautifulSoup` libraries.

2. **Data Transformation**: 
   - The extracted data is then transformed to include market capitalization values in GBP, EUR, and INR. This transformation uses exchange rates provided in the [exchange rate](https://github.com/user-attachments/files/17577222/exchange_rate.csv) file.

3. **Data Loading**: 
   - The transformed data is saved in two formats: as a [CSV file](https://github.com/user-attachments/files/17577223/Largest_banks_data.csv) and in an [SQLite database](https://github.com/user-attachments/files/17577238/Banks.zip). This enables flexible data analysis and retrieval.

4. **Logging**: 
   - Each step of the ETL process is logged with timestamps in [log text file](https://github.com/user-attachments/files/17577232/code_log.txt), providing a clear trail of the workflow for troubleshooting and record-keeping.

## Terminal Output
The following SQL queries were executed to retrieve data from the database:

- **Query 1**:
  ```sql
  SELECT * FROM Largest_banks
- **Query 2**:
  ```sql
  SELECT AVG(MC_GBP_Billion) FROM Largest_banks
- **Query 3**:
  ```sql
  SELECT Name FROM Largest_banks LIMIT 5

Below is a screenshot of the terminal output displaying the results of these queries:
![Terminal Output Screenshot](https://github.com/user-attachments/assets/ed6de47c-ed30-47ab-8a13-d7489bd6b91c)
