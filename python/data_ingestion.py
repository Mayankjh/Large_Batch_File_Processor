import csv
from os import read
import sqlite3
import glob
import shutil
import util.sql_util as su
import multiprocess as mp
import sys


def chunk_generator(batch_size, source_file):
        print('reading data')
        print(source_file)
        with open(source_file, 'r') as f:
            csv_reader = csv.reader(f, delimiter=',')
            next(csv_reader, None)
            batch_data = []
            batch_count = 0
            for row in csv_reader:
                batch_data.append([v if v != '' else None for v in row])
                batch_count += 1
                if batch_count % batch_size == 0:
                    yield batch_data
                    batch_data = []
            if batch_data:
                yield batch_data

def data_ingestion(product_temp,product_tbl):
    file_loc = glob.glob(r'./../data/*.csv')
    batch_size = 5000
    for file in file_loc:
        chunk_gen = chunk_generator(batch_size,file)
        print("Ingesting File :", file)
        pool = mp.Pool(mp.cpu_count()-1)
        pool.map(su.SqlUtil.data_insert, chunk_gen)
        pool.close()
        print("Inserting data complete for file: ", file)
        print('Insert to Stage Complete complete. Insert to Prod Table Start')
        result = su.SqlUtil.execute_custom_query("SELECT COUNT(*) FROM {product_tbl}".format(product_tbl=product_tbl))
        su.SqlUtil.get_cur()
        count = result.fetchone()[0]
        su.SqlUtil.close_conn()
        if count == 0:
            print("Records in Prod Table :", count)
            insert_query = """INSERT INTO {product_tbl} SELECT name,sku,description FROM {product_temp}""".format(product_temp=product_temp,product_tbl=product_tbl)
            print('Bulk Insert from stage to prod table (One time Full Load)')
            su.SqlUtil.execute_custom_query(insert_query)
            print("Building PROD_AGG Table")
            agg_query="CREATE TABLE PRODUCT_AGG as SELECT name,COUNT(name) as PROD_COUNT from {product_tbl} group by name".format(product_tbl=product_tbl)
            su.SqlUtil.execute_custom_query(agg_query)
            print("Build for PROD_AGG Table complete")
        elif count>1:
            print("Records in Prod Table :", count)
            print('Inserting New Records to Prod')
            new_records = """INSERT INTO {product_tbl} SELECT temp_tbl.name, temp_tbl.sku, temp_tbl.description
                            FROM {product_temp} temp_tbl left join {product_tbl} prod_tbl on temp_tbl.name = prod_tbl.name and temp_tbl.sku = prod_tbl.sku and temp_tbl.description = prod_tbl.description
                            WHERE prod_tbl.sku is NULL""".format(product_temp=product_temp,product_tbl=product_tbl)
            su.SqlUtil.execute_custom_query(new_records)
            su.SqlUtil.execute_custom_query("DROP TABLE PRODUCT_AGG")
            print("Updating Aggregated View Table")
            agg_query="CREATE TABLE PRODUCT_AGG as SELECT name,COUNT(name) as PROD_COUNT from {product_tbl} group by name".format(product_tbl=product_tbl)
            su.SqlUtil.execute_custom_query(agg_query)
            print("Update for PROD_AGG Table complete")

        print('Prod Table insertion Complete. Truncating Stage Table')
        su.SqlUtil.execute_custom_query("DELETE FROM {product_temp}".format(product_temp=product_temp))
        print('File Ingestion Succeeded. Please query {0} Table to check the results.'.format(product_tbl))
        su.SqlUtil.close_conn()
        print("Archiving File...")
        shutil.move(file, "./../data/archive/"+file.split("\\")[-1])
        print("Archival of File Complete.")

def update_sku(product_tbl):    
        sku = input("Enter the sku that you would like to update\n")
        check_record = """SELECT COUNT(*) FROM {product_tbl} where sku = '{sku}'""".format(sku=sku,product_tbl=product_tbl)
        result = su.SqlUtil.execute_custom_query(check_record)
        su.SqlUtil.get_cur()
        count = int(result.fetchone()[0])
        su.SqlUtil.close_conn()
        if count == 0:
            print("The given sku does not exist. Exiting Program")
            return False
        else:
            name = input("Enter the name of the sku that you would like to update\n")
            check_record = """SELECT COUNT(1) FROM {2} where sku = '{0}' and name = '{1}'""".format(sku, name, product_tbl)
            result = su.SqlUtil.execute_custom_query(check_record)
            su.SqlUtil.get_cur()
            count = int(result.fetchone()[0])
            su.SqlUtil.close_conn()
            if count == 0:
                print("The given name does not exist. Exiting Program")
                return False
            description = input("Enter the new description of the sku\n")
            update_query = """UPDATE {3} SET description = '{0}' WHERE sku = '{1}' and name = '{2}'""".format(description, sku, name, product_tbl)
            su.SqlUtil.execute_custom_query(update_query)
            print("The sku has been updated successfully.")
        
    













    