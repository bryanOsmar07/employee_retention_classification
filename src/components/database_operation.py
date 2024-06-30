import sqlite3
import csv
from os import listdir
import shutil
import os
from dataclasses import dataclass

import sys
from src.exception import CustomException
from src.logger import logging

@dataclass
class DataIngestionConfig:
    raw_data_path: str=os.path.join('artifacts',"data.csv") 



class DatabaseOperation:
    """
    *****************************************************************************
    *
    * file_name:       DatabaseOperation.py
    * version:        1.0
    * author:         BryanOsmar
    * creation date:  28-JUN-2024
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * BryanOsmar      28-JUN-2024    1.0      initial creation
    *
    *
    * description:    Class to handle database operations
    *
    ****************************************************************************
    """

    def __init__(self,run_id,data_path,mode):
        self.run_id = run_id
        self.data_path = data_path
        self.ingestion_config=DataIngestionConfig()

    def database_connection(self,database_name):
        """
        * method: database_connection
        * description: method to build database connection
        * return: Connection to the DB
        *
        *
        * Parameters
        *   database_name:
        """
        db_path = 'artifacts/database/'
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        try:
            conn = sqlite3.connect(os.path.join(db_path, database_name + '.db'))
            logging.info("Opened %s database successfully" % database_name)
        except Exception as e:
            logging.info("Error while connecting to database")
            raise CustomException(e, sys)
        return conn

    def create_table(self,database_name,table_name, column_names):
        """
        * method: create_table_db
        * description: method to create database table
        * return: none
        *
        *
        * Parameters
        *   database_name:
        *   column_names:
        """

        try:
            logging.info("Start of Creating Table...")
            conn = self.database_connection(database_name)

            if (database_name == 'prediction'):
                conn.execute("DROP TABLE IF EXISTS '"+table_name+"';")

            c=conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = '"+table_name+"'")
            if c.fetchone()[0] ==1:
                conn.close()
                logging.info('Tables created successfully')
                logging.info("Closed %s database successfully" % database_name)
            else:
                for key in column_names.keys():
                    type = column_names[key]

                    #in try block we check if the table exists, if yes then add columns to the table
                    # else in catch block we will create the table --training_raw_data_t
                    try:
                        conn.execute("ALTER TABLE "+table_name+" ADD COLUMN {column_name} {dataType}".format(column_name=key,dataType=type))
                        logging.info("ALTER TABLE "+table_name+" ADD COLUMN")
                    except:
                        conn.execute("CREATE TABLE  "+table_name+" ({column_name} {dataType})".format(column_name=key, dataType=type))
                        logging.info("CREATE TABLE "+table_name+" column_name")
                conn.close()
            logging.info('End of Creating Table...')
        except Exception as e:
            logging.info('Exception raised while Creating Table')
            raise CustomException(e,sys)

    def insert_data(self,database_name,table_name):
        """
        * method: insert
        * description: method to insert data into table
        * return: none
        *
        *
        * Parameters
        *   database_name:
        """
        conn = self.database_connection(database_name)
        good_data_path= self.data_path
        bad_data_path = self.data_path+'_rejects'
        only_files = [f for f in listdir(good_data_path)]
        logging.info('Start of Inserting Data into Table...')
        for file in only_files:
            try:
                with open(good_data_path+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter=",")
                    for line in enumerate(reader):
                        #self.logger.info(" %s: nu!!" % line[1])
                        to_db=''
                        for list_ in (line[1]):
                            try:
                                to_db = to_db +",'"+list_+"'"
                            except Exception as e:
                                raise e
                        #self.logger.info(" %s: list_!!" % to_db.lstrip(','))
                        to_db=to_db.lstrip(',')
                        conn.execute("INSERT INTO "+table_name+" values ({values})".format(values=(to_db)))
                        conn.commit()

            except Exception as e:
                conn.rollback()
                logging.info('Exception raised while Inserting Data into Table')
                shutil.move(good_data_path+'/' + file, bad_data_path)
                conn.close()
                raise CustomException(e,sys)
        conn.close()
        logging.info('End of Inserting Data into Table...')

    def export_csv(self,database_name,table_name):
        """
        * method: export_csv
        * description: method to select data from table in export into csv
        * return: none
        *
        *
        * Parameters
        *   database_name:
        """
        self.file_from_db = self.data_path+str('_validation/')
        self.file_name = 'InputFile.csv'
        try:
            logging.info('Start of Exporting Data into CSV...')
            conn = self.database_connection(database_name)
            sqlSelect = "SELECT *  FROM "+table_name+""
            cursor = conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]
            #Make the CSV ouput directory
            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)
            # Open CSV file for writing.
            csv_file = csv.writer(open(self.file_from_db + self.file_name, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            # Add the headers and data to the CSV file.
            csv_file.writerow(headers)
            csv_file.writerows(results)
            logging.info('End of Exporting Data into CSV...')
        except Exception as e:
            logging.info('Exception raised while Exporting Data into CSV')
            raise CustomException(e,sys)