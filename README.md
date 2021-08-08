# Large_Batch_File_Processor
A python based framework to handle large csv based ingestion processes

# Tech Stack Used:
  [1] DB : SQLlite3
  [2] Language: Python 3.x
  [3] OS: Windows 10

# Required Libraries:
1. sqlite3
2. multiprocess
3. google_drive_downloader
4. shutil
5. yaml

# Table & Schema Details:
1. `PRODUCT_TEMP`: Temporary Table to store stagging data 
2. `PRODUCT_TABLE`: Final Product Table
3. `PRODUCT_AGG`: Product aggerigation table containing name and product counts.

## Create Table Commands
1. `CREATE TABLE PRODUCT_TEMP (NAME TEXT, SKU TEXT ,DESCRIPTION TEXT);` -- Required before code run
2. `CREATE TABLE PRODUCT_TBL (NAME TEXT, SKU TEXT ,DESCRIPTION TEXT);`  -- Required before code run
3. `CREATE TABLE PRODUCT_AGG as SELECT name,COUNT(name) as PROD_COUNT from PRODUCT_TBL group by name;` --Created during code run

# Project Workflow:
1. Download file from G-drive location to required directory. The process uses python library google_drive_downloader to do so. User is required to provide ID and file name details for the process. The process is flexible enough to handle multiple product.csv within same zip.
2. Ingestion process starts reading all files downloaded in the data folder and breaking them into chunks and then inserting them into stagging table. If there are no records in final product table already. The process will do one time bulk load. If there are records in the final product table and not the first time run, It will compare final and stagging table to insert new records in the final table. 
3. Once the above process is over the aggregated table will be updated with new records. It is done in truncate load/full refresh fashion.
4. The ingestion process will come to an end moving downloaded files to an archive location.
5. The process also allows you to update records in product table on one to one basis.


# Steps to Run the Program:
`python main.py ./../configuration/conn_string.yaml`

# Points that have been acheived
1. Your code should follow concept of OOPS
2. Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.- Achieved using Multi-Processing
3. Support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it)
4. All product details are to be ingested into a single table
5. An aggregated table on above rows with `name` and `no. of products` as the columns

# Things to be improved

1. Induce exception handeling and logging in code.
2. The current code was more focused to meet the objective can be improved and optimized further.
3. The database currently used in the code is SQLlite3 can be replaced with more robust databases as per requirements
4. Automate code using orchestrator like docker.

