# Nifty50 Data Analysis  

**Project Link:** [Nifty50 Data Analysis](https://lookerstudio.google.com/u/1/reporting/bbafb746-adf8-4a30-b7ed-65a809c9215b/page/JVgvE)  

Welcome to the **Nifty50 Data Analysis** project. The objective of this project is to leverage the information of the top 50 stocks in **NIFTY**, calculate **price changes** and **buy-sell ratios**, and enable users to understand the current stock trends, including top gainers and losers within a specific timeframe.  

The project fetches data using the **nsepython** library, applies transformations, stores the data in a **Data Warehouse (DWH)**, and presents findings on a **dashboard** as outlined in the architecture below.  

---

## **Architecture**  

![image](https://github.com/user-attachments/assets/1cc8ed3c-40ee-45ff-9956-372faf914f04)  

The data is loaded and refreshed daily at **6 AM IST (Monday to Friday).**  

1. **Fetching Data:**  
   - A **Python script** runs on **Google Cloud Functions** to fetch data and store it in **Parquet format** in the `/raw` folder of a **Google Cloud Storage (GCS) bucket**.  
   ![image](https://github.com/user-attachments/assets/90de55b2-ada3-4409-a45a-ac4299a5f9f6)  

2. **Data Processing (BigQuery + PySpark):**  
   - The file is picked from **GCS**, and a **PySpark job** runs on **BigQuery** (using **Dataproc Serverless** in the background).  
   - The data is divided into **three Spark DataFrames**:  
     - **Company DataFrame**: Contains stock names, industries, and symbols.  
     - **Stock DataFrame**: Contains all stock KPIs and metrics.  
     - **Date DataFrame**: Stores date-related information.  
   - These DataFrames are loaded into **three staging tables** in **BigQuery**.  

   ![image](https://github.com/user-attachments/assets/5bb41a77-2594-45ca-8d34-8db0d97f9d48)  
   ![image](https://github.com/user-attachments/assets/45bf1bd8-33b5-4161-aef6-649547519cea)  
   ![image](https://github.com/user-attachments/assets/97925e1a-46dd-4aa7-bca9-9e21f01aaa7f)  

3. **Incremental Load to DWH:**  
   - The data from **staging tables** is **incrementally merged** into the **three Data Warehouse (DWH) tables** using **SQL queries**.  

4. **Transformations & KPI Calculations:**  
   - The **DWH tables** undergo further transformations, and **KPIs** are created using **SQL**.  
   - The transformed data is loaded into **two Data Mart (DTM) tables**, each serving a different report:  
     - **Price Change Report**  
     - **Buy-Sell Report**  
   - **DTM tables are truncated and loaded daily.**  

   ![image](https://github.com/user-attachments/assets/9ee82bed-28a9-4bb6-9701-c738d1c8937f)  
   ![image](https://github.com/user-attachments/assets/1d807a9a-de0a-4624-82e6-4807809eccf9)  

5. **Data Visualization in Looker Studio:**  
   - The data from **DTM tables** is joined with **Company and Date DWH tables** to form **blended data**.  
   - The final dataset is used to create **two reports in Google Looker Studio**.
     ![image](https://github.com/user-attachments/assets/bbab37d5-8449-4b07-882d-bcbfbcd89d8b)
     ![image](https://github.com/user-attachments/assets/3a5008f1-82f2-456e-b7f9-d9288803c0f2)


6. **Data Archival:**  
   - A **Cloud Function** moves **daily incremental data** from the **raw folder** to the **archive folder** in **Google Cloud Storage**.  

7. **Orchestration:**
   - All these processes are orchestrated using Google workflows 
     


## **Tools**  

- **Orchestration:** Google Workflows  
- **Data Ingestion (Batch):** Python, Google Cloud Functions  
- **Staging:** Google Cloud Storage  
- **Data Warehouse:** Google BigQuery  
- **Transformations:** PySpark, SQL  
- **Visualization:** Google Looker Studio  

