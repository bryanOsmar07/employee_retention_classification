from datetime import datetime
import random
import pickle
import os
import shutil
from src.logger import logging
from src.exception import CustomException
import sys

class Config:
    """
    *****************************************************************************
    *
    * filename:       utils.py
    * version:        1.0
    * author:         bryanOsmar
    * creation date:  28-JUN-2024
    *
    * change history:
    *
    *
    *
    * description:    Class for configuration instance attributes
    *
    ****************************************************************************
    """

    def __init__(self):
        self.training_data_path = 'data/training_data'
        self.training_database = 'training'
        self.prediction_data_path = 'data/prediction_data'
        self.prediction_database = 'prediction'

    def get_run_id(self):
        """
        * method: get_run_id
        * description: method to generate run id
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H%M%S")
        return str(self.date)+"_"+str(self.current_time)+"_"+str(random.randint(100000000, 999999999))
    

class FileOperation:
    """
    *****************************************************************************
    *
    * file_name:       FileOperation.py
    * version:        1.0
    * author:         CODESTUDIO
    * creation date:  05-MAY-2020
    *
    * change history:
    *
    *
    *
    * description:    Class for file operation
    *
    ****************************************************************************
    """

    def __init__(self,run_id,data_path,mode):
        self.run_id = run_id
        self.data_path = data_path

    def save_model(self,model,file_name):
        """
        * method: save_model
        * description: method to save the model file
        * return: File gets saved
        *
        *
        * Parameters
        *   model:
        *   file_name:
        """
        try:
            logging.info('Start of Save Models')
            path = os.path.join('apps/models/',file_name) #create seperate directory for each cluster
            if os.path.isdir(path): #remove previously existing models for each clusters
                shutil.rmtree('apps/models')
                os.makedirs(path)
            else:
                os.makedirs(path) #
            with open(path +'/' + file_name+'.sav',
                      'wb') as f:
                pickle.dump(model, f) # save the model to file
            logging.info('Model File '+file_name+' saved')
            logging.info('End of Save Models')
            return 'success'
        except Exception as e:
            logging.info('Exception raised while Save Models')
            raise CustomException(e,sys)

    def load_model(self,file_name):
        """
        * method: load_model
        * description: method to load the model file
        * return: File gets saved
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * bcheekati       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   file_name:
        """
        try:
            logging.info('Start of Load Model')
            with open('apps/models/' + file_name + '/' + file_name + '.sav','rb') as f:
                logging.info('Model File ' + file_name + ' loaded')
                logging.info('End of Load Model')
                return pickle.load(f)
        except Exception as e:
            logging.info('Exception raised while Loading Model')
            raise CustomException(e,sys)

    def correct_model(self,cluster_number):
        """
        * method: correct_model
        * description: method to find best model
        * return:  The Model file
        *
        *
        * Parameters
        *   cluster_number:
        """
        try:
            logging.info('Start of finding correct model')
            self.cluster_number= cluster_number
            self.folder_name='apps/models'
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str( self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            logging.info('End of finding correct model')
            return self.model_name
        except Exception as e:
            logging.info('Exception raised while finding correct model' )
            raise CustomException(e,sys)