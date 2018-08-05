import os.path
import csv
import numpy as np
import pandas as pd
import json
import pika
from flatten_json import flatten
from traffic.mdm_logging import *

# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files

setup_logging()
logger = logging.getLogger(__name__)

class HelperClass:

    # get csv and store in DF

    def decodeCsv(self, body):
        file_name = body.decode().split('.csv')[0]
        message = body.decode().split('.csv')[1]
        filename = '{}.csv'.format(file_name)
        with open(filename, 'w', encoding='utf-8') as write_csv:
            # with open('{}.csv'.format(file_name), 'w') as write_csv:
            write_csv.write(message)
        return filename

    def csvToDF(self, filename):
        df = pd.read_csv(filename, error_bad_lines=False)
        return df

    def DFToCsv(self, filename, csv_filename):
        csv = filename.to_csv(csv_filename)
        return csv

    def DFToDict(self, filename):
        dict = filename.to_dict('dict')
        return dict

    def DictToDF(self, filename):
        df = pd.DataFrame.from_dict(filename)
        return df

    # does not work yet, prints an empty JSON, though the df is definitely not empty
    def DFtoJSON(self, df):
        df.to_json('temp.json', orient='table')
        data = json.loads('temp.json')
        print('Print data: ')
        print(data)
        return data

    def JSONtoDF(self):
        df = pd.read_json('temp.json', orient='split')
        return df
