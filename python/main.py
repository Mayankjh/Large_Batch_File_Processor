import logging
import yaml
import util.data_processor as data_pre
import data_ingestion as ingest
import sys
"""
MAIN keyS
"""
def run(product_temp,product_tbl,**config):
    while True:
        print("""Please see the below keys to choose how to proceed further:
        Press 0: Download and Unzip files to be ingested
        Press 1: For Inserting Data into Table from CSV File
        Press 2: To Update the existing sku
        Press 3: To Exit the Program.""")
        key = int(input('Please input the right key to proceed further\n'))
        if key==0:
            print(" Keep you googledrive file link ready e.g. https://drive.google.com/file/d/11ACp03VCQY5NElctMq7F5zn23jKrqTZI/view?usp=sharing")
            fileid = input(" Enter Google drive File Id to be downloaded e.g. '11ACp03VCQY5NElctMq7F5zn23jKrqTZI' :")
            file_name = input(" Enter Google drive File Name to be downloaded e.g. 'products.csv.gz' :")
            data_pre.getfiles(fileid,file_name)
        elif key == 1:
            # insert_to_prod(stage_table,prod_table,sql_execute)
            ingest.data_ingestion(product_temp,product_tbl)
            print("CSV File Ingested to Table Successfully")
        elif key == 2:
            pass
            ingest.update_sku(product_tbl)
        elif key == 3:
            print("Exiting Program...")
            break
        else:
            print(" Enter a valid key. Please enter key again.")



if __name__ == '__main__':
    config_path = sys.argv[1]
    product_temp = 'PRODUCT_TEMP'
    product_tbl = 'PRODUCT_TBL'
    with open(config_path) as json_data:
        prop = yaml.load(json_data)   
    run(product_temp,product_tbl,**prop)
