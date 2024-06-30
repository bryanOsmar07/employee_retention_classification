import json
from os import listdir
import sys
import shutil
import pandas as pd
from datetime import datetime
import os
from dataclasses import dataclass
from sklearn.model_selection import train_test_split
from src.components.database_operation import DatabaseOperation
from src.logger import logging
from src.exception import CustomException

@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts',"train.csv") 
    test_data_path: str=os.path.join('artifacts',"test.csv") 
    raw_data_path: str=os.path.join('artifacts',"data.csv") 

class LoadValidate:
    """
    *****************************************************************************
    *
    * filename:       LoadValidate.py
    * version:        1.0
    * author:         bryanOsmar
    * creation date:  28-JUN-2024
    *
    * change history:
    *
    *
    * description:    Class to load, validate and transform the data
    *
    ****************************************************************************
    """

    def __init__(self,run_id,data_path,mode):
        self.run_id = run_id
        self.data_path = data_path
        self.dbOperation = DatabaseOperation(self.run_id, self.data_path, mode)

    def values_from_schema(self,schema_file):
        """
        * method: values_from_schema
        * description: method to read schema file
        * return: column_names, Number of Columns
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Reading values From Schema...')
            with open('artifacts/database/'+schema_file+'.json', 'r') as f:
                dic = json.load(f)
                f.close()
            column_names = dic['ColName']
            number_of_columns = dic['NumberofColumns']
            logging.info('End of Reading values From Schema...')
        except Exception as e:
            logging.info('Exception raised while Reading values From Schema: %s' % e)
            raise CustomException(e,sys)
        return column_names, number_of_columns

    def validate_column_length(self,number_of_columns):
        """
        * method: validate_column_length
        * description: method to validates the number of columns in the csv files
        * return: none
        *
        *
        * Parameters
        *   NumberofColumns:
        """
        try:
            logging.info('Start of Validating Column Length...')
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path+'/'+ file)
                if csv.shape[1] == number_of_columns:
                    pass
                else:
                    shutil.move(self.data_path +'/'+ file, self.data_path+'_rejects')
                    self.logger.info("Invalid Columns Length :: %s" % file)

            logging.info('End of Validating Column Length...')
        except Exception as e:
            logging.info('Exception raised while Validating Column Length')
            raise CustomException(e,sys)

    def validate_missing_values(self):
        """
        * method: validate_missing_values
        * description: method to validates if any column in the csv file has all values missing.
        *              If all the values are missing, the file is not suitable for processing. it has to be moved to bad file
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Validating Missing Values...')
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path+'/'+ file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move(self.data_path+'/' + file,self.data_path+'_rejects')
                        logging.info("All Missing Values in Column :: %s" % file)
                        break

            logging.info('End of Validating Missing Values...')
        except Exception as e:
            logging.info('Exception raised while Validating Missing Values')
            raise CustomException(e,sys)


    def replace_missing_values(self):
        """
        * method: replace_missing_values
        * description: method to replaces the missing values in columns with "NULL"
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Replacing Missing Values with NULL...')
            only_files = [f for f in listdir(self.data_path)]
            for file in only_files:
                csv = pd.read_csv(self.data_path + "/" + file)
                csv.fillna('NULL', inplace=True)
                csv.to_csv(self.data_path + "/" + file, index=None, header=True)
                logging.info('%s: File Transformed successfully!!' % file)
            logging.info('End of Replacing Missing Values with NULL...')
        except Exception as e:
            logging.info('Exception raised while Replacing Missing Values with NULL')
            raise CustomException(e,sys)

    def archive_old_files(self):
        """
        * method: archive_old_rejects
        * description: method to archive rejected files
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            logging.info('Start of Archiving Old Rejected Files...')
            source = self.data_path+'_rejects/'
            if os.path.isdir(source):
                path = self.data_path+'_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path+'/reject_' + str(date)+"_"+str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)

            logging.info('End of Archiving Old Rejected Files...')

            logging.info('Start of Archiving Old Validated Files...')
            source = self.data_path + '_validation/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/validation_' + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)

            logging.info('End of Archiving Old Validated Files...')

            logging.info('Start of Archiving Old Processed Files...')
            source = self.data_path + '_processed/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/processed_' + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)

            logging.info('End of Archiving Old Processed Files...')

            logging.info('Start of Archiving Old Result Files...')
            source = self.data_path + '_results/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/results_' + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)

            logging.info('End of Archiving Old Result Files...')
        except Exception as e:
            logging.info('Exception raised while Archiving Old Rejected Files')
            raise CustomException(e,sys)

    def move_processed_files(self):
        """
        * method: move_processed_files
        * description: method to move processed files
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Moving Processed Files...')
            for file in listdir(self.data_path):
                shutil.move(self.data_path + '/' + file, self.data_path + '_processed')
                logging.info("Moved the already processed file %s" % file)

            logging.info('End of Moving Processed Files...')
        except Exception as e:
            logging.info('Exception raised while Moving Processed Files')
            raise CustomException(e,sys)

    def validate_trainset(self):
        """
        * method: validate
        * description: method to validate the data
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Data Load, validation and transformation')
            # archive old  files
            self.archive_old_files()
            # extracting values from training schema
            column_names, number_of_columns = self.values_from_schema('schema_train')
            # validating column length in the file
            self.validate_column_length(number_of_columns)
            # validating if any column has all values missing
            self.validate_missing_values()
            # replacing blanks in the csv file with "Null" values
            self.replace_missing_values()
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dbOperation.create_table('training','training_raw_data_t',column_names)
            # insert csv files in the table
            self.dbOperation.insert_data('training','training_raw_data_t')
            # export data in table to csv file
            self.dbOperation.export_csv('training','training_raw_data_t')
            # move processed files
            self.move_processed_files()
            logging.info('End of Data Load, validation and transformation')
        except Exception as e:
            logging.info('Unsuccessful End of Data Load, validation and transformation')
            raise CustomException(e,sys)

    def validate_predictset(self):
        """
        * method: validate
        * description: method to validate the predict data
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Data Load, validation and transformation')
            # archive old rejected files
            self.archive_old_files()
            # extracting values from schema
            column_names, number_of_columns = self.values_from_schema('schema_predict')
            # validating column length in the file
            self.validate_column_length(number_of_columns)
            # validating if any column has all values missing
            self.validate_missing_values()
            # replacing blanks in the csv file with "Null" values
            self.replace_missing_values()
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dbOperation.create_table('prediction','prediction_raw_data_t', column_names)
            # insert csv files in the table
            self.dbOperation.insert_data('prediction','prediction_raw_data_t')
            # export data in table to csv file
            self.dbOperation.export_csv('prediction','prediction_raw_data_t')
            # move processed files
            self.move_processed_files()
            logging.info('End of Data Load, validation and transformation')
        except Exception as e:
            logging.info('Unsuccessful End of Data Load, validation and transformation')
            raise CustomException(e,sys)