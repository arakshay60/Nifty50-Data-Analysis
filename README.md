# Nifty50-Data-Analysis
Project Link: [Nifty50 Data Analysis](https://lookerstudio.google.com/u/1/reporting/bbafb746-adf8-4a30-b7ed-65a809c9215b/page/JVgvE)

Welcome to Nifty50 Data Analysis project. The objective of the project is to leverage the information of top 50 stocks in NIFTY and calculate its price change as well as buy sell ratio enabling the users to understand the current trend of the stocks and the top losers and gainers within a specific timeframe. The project intends to fetch data from the nsepython library, apply transformations to it, store the data in DWH and present the findings on a dashboard as outlined in the architecture below.

Architecture

![image](https://github.com/user-attachments/assets/1cc8ed3c-40ee-45ff-9956-372faf914f04)

The data is loaded and refreshed daily at 6am IST from Monday to Friday

1.Fetching Data: A python script is run using google cloud functions which loads data in parquet format to the "/raw" folder in google cloud storage (GCS) bucket.
![image](https://github.com/user-attachments/assets/90de55b2-ada3-4409-a45a-ac4299a5f9f6)

2.The file is taken up from GCS, and a pyspark code is run on BigQuery which uses Dataproc serverless in the background. The data is divided into 3 spark dataframes, Company, Stock and Date . Company df contains information on the full name of the stock, its industy and stock symbol. Stock df contains all the KPI's and metrics for each stock. All these dataframes are then loaded into 3 staging tables in Bigquery.
![image](https://github.com/user-attachments/assets/5bb41a77-2594-45ca-8d34-8db0d97f9d48)

![image](https://github.com/user-attachments/assets/45bf1bd8-33b5-4161-aef6-649547519cea)

![image](https://github.com/user-attachments/assets/97925e1a-46dd-4aa7-bca9-9e21f01aaa7f)

3.The data in the staging tables are then incrementally loaded to their corresponding 3 DWH tables using SQL queries

4.Transformations are applied on the data from DWH tables and KPI's are created using SQL and loaded into 2 DTM tables each for the 2 reports, i.e, (Price Change Report & Buy-Sell Report). The DTM tables are truncated and loaded everyday.

![image](https://github.com/user-attachments/assets/9ee82bed-28a9-4bb6-9701-c738d1c8937f)

![image](https://github.com/user-attachments/assets/1d807a9a-de0a-4624-82e6-4807809eccf9)



5.The data from 2 DTM tables are joined with Company and Date DWH tables to form blended data in Looker Studio. This data is then used to create the 2 reports.

