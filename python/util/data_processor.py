from google_drive_downloader import GoogleDriveDownloader as gdd
import gzip
import shutil
import time
import glob
import os

def getfiles(file_id,file_name):
    
    destination_path = './../data/{file_name}'.format(file_name=file_name)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    # Downloading file...
    gdd.download_file_from_google_drive(file_id=file_id,
                                    dest_path=destination_path,
                                    unzip=False)
    #unzipping gz file
    with gzip.open(destination_path, 'rb') as f_in:
            with open('./../data/products_{timestamp}.csv'.format(timestamp=timestr), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    os.remove(destination_path)
    print("File Downloaded and Unzipped successfully")
    print('Your Files are ready for Ingestion',glob.glob("./../data/*.csv"))
                