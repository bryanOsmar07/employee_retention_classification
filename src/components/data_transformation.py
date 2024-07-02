import pandas as pd
import numpy as np
import sys
import json
from sklearn.impute import KNNImputer
from src.logger import logging
from src.exception import CustomException

class Preprocessor:
    """
    *****************************************************************************
    *
    * filename:       Preprocessor.py
    * version:        1.0
    * author:         bryanOsmar
    * creation date:  28-JUN-2024
    *
    * change history:
    *
    *
    *
    * description:    Class to pre-process training and predict dataset
    *
    ****************************************************************************
    """

    def __init__(self,run_id,data_path,mode):
        self.run_id = run_id
        self.data_path = data_path

    def get_data(self):
        """
        * method: get_data
        * description: method to read datafile
        * return: A pandas DataFrame
        *
        *
        * Parameters
        *   none:
        """
        try:
            # reading the data file
            logging.info('Start of reading dataset...')
            self.data= pd.read_csv(self.data_path+'_validation/InputFile.csv')
            logging.info('End of reading dataset...')
            return self.data
        except Exception as e:
            logging.exception('Exception raised while reading dataset')
            raise CustomException(e,sys)

    def drop_columns(self,data,columns):
        """
        * method: drop_columns
        * description: method to drop columns
        * return: A pandas DataFrame after removing the specified columns.
        *
        *
        * Parameters
        *   data:
        *   columns:
        """
        self.data=data
        self.columns=columns
        try:
            logging.info('Start of Droping Columns...')
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            logging.info('End of Droping Columns...')
            return self.useful_data
        except Exception as e:
            logging.info('Exception raised while Droping Columns')
            raise CustomException(e,sys)

    def is_null_present(self,data):
        """
        * method: is_null_present
        * description: method to check null values
        * return: Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * bcheekati       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        self.null_present = False
        try:
            logging.info('Start of finding missing values...')
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in self.null_counts:
                if i>0:
                    self.null_present=True
                    break
            if(self.null_present): # write the logs to see which columns have null values
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv(self.data_path+'_validation/'+'null_values.csv') # storing the null column information to file
            logging.info('End of finding missing values...')
            return self.null_present
        except Exception as e:
            logging.info('Exception raised while finding missing values')
            raise CustomException(e,sys)

    def impute_missing_values(self, data):
        """
        * method: impute_missing_values
        * description: method to impute missing values
        * return: none
        *
        *
        * Parameters
        *   data:
        """
        self.data= data
        try:
            logging.info('Start of imputing missing values...')
            imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Data frame
            self.new_data=pd.DataFrame(data=self.new_array, columns=self.data.columns)
            logging.info('End of imputing missing values...')
            return self.new_data
        except Exception as e:
            logging.info('Exception raised while imputing missing values')
            raise CustomException(e,sys)

    def feature_encoding(self, data):
        """
        * method: feature_encoding
        * description: method to impute missing values
        * return: none
        *
        *
        * Parameters
        *   data:
        """
        try:
            logging.info('Start of feature encoding...')
            self.new_data = data.select_dtypes(include=['object']).copy()
            # Using the dummy encoding to encode the categorical columns to numerical ones
            for col in self.new_data.columns:
                self.new_data = pd.get_dummies(self.new_data, columns=[col], prefix=[col], drop_first=True)

            logging.info('End of feature encoding...')
            return self.new_data
        except Exception as e:
            logging.exception('Exception raised while feature encoding:' + str(e))
            raise CustomException(e,sys)


    def split_features_label(self, data, label_name):
        """
        * method: split_features_label
        * description: method to separate features and label
        * return: none
        *
        *
        * Parameters
        *   data:
        *   label_name:
        """
        self.data =data
        try:
            logging.info('Start of splitting features and label ...')
            self.X=self.data.drop(labels=label_name,axis=1) # drop the columns specified and separate the feature columns
            self.y=self.data[label_name] # Filter the Label columns
            logging.info('End of splitting features and label ...')
            return self.X,self.y
        except Exception as e:
            logging.info('Exception raised while splitting features and label')
            raise CustomException(e,sys)

    def final_predictset(self,data):
        """
        * method: final_predictset
        * description: method to build final predict set by adding additional encoded column with value as 0
        * return: column_names, Number of Columns
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of building final predictset...')
            with open('apps/database/columns.json', 'r') as f:
                data_columns = json.load(f)['data_columns']
                f.close()
            df = pd.DataFrame(data=None, columns=data_columns)
            df_new = pd.concat([df, data], ignore_index=True,sort=False)
            data_new = df_new.fillna(0)
            logging.info('End of building final predictset...')
            return data_new
        except Exception as e:
            logging.info('Exception raised while building final predictset')
            raise CustomException(e,sys)


    def preprocess_trainset(self):
        """
        * method: preprocess_trainset
        * description: method to pre-process training data
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Preprocessing...')
            # get data into pandas data frame
            data=self.get_data()
            # drop unwanted columns
            data=self.drop_columns(data,['empid'])
            # handle label encoding
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            # drop categorical column
            data = self.drop_columns(data, ['salary'])
            # check if missing values are present in the data set
            is_null_present = self.is_null_present(data)
            # if missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.impute_missing_values(data)  # missing value imputation
            # create separate features and labels
            self.X, self.y = self.split_features_label(data, label_name='left')
            logging.info('End of Preprocessing...')
            return self.X, self.y
        except Exception as e:
            logging.info('Unsuccessful End of Preprocessing...')
            raise CustomException(e,sys)

    def preprocess_predictset(self):
        """
        * method: preprocess_predictset
        * description: method to pre-process prediction data
        * return: none
        *
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Preprocessing...')
            # get data into pandas data frame
            data=self.get_data()
            # drop unwanted columns
            #data=self.drop_columns(data,['empid'])
            # handle label encoding
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            # drop categorical column
            data = self.drop_columns(data, ['salary'])
            # check if missing values are present in the data set
            is_null_present = self.is_null_present(data)
            # if missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.impute_missing_values(data)  # missing value imputation

            data = self.final_predictset(data)
            logging.info('End of Preprocessing...')
            return data
        except Exception as e:
            logging.info('Unsuccessful End of Preprocessing...')
            raise CustomException(e,sys)


    def preprocess_predict(self,data):
        """
        * method: preprocess_predict
        * description: method to pre-process prediction data
        * return: none
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * bcheekati       05-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   none:
        """
        try:
            logging.info('Start of Preprocessing...')
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            # drop categorical column
            data = self.drop_columns(data, ['salary'])
            # check if missing values are present in the data set
            is_null_present = self.is_null_present(data)
            # if missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.impute_missing_values(data)  # missing value imputation

            data = self.final_predictset(data)
            logging.info('End of Preprocessing...')
            return data
        except Exception as e:
            logging.info('Unsuccessful End of Preprocessing...')
            raise CustomException(e,sys)
