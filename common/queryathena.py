"""Module for Querying Athena"""
import time
import io
import os
import boto3
import pandas as pd

class QueryAthena:
    """Class for Querying Athena"""

    def __init__(self,database,folder,bucket,region,access_key,secret_key,query):
        self.database = database
        self.folder = folder
        self.bucket =  bucket
        self.s3_output =  's3://' + self.bucket + '/' + self.folder
        self.region_name = region
        self.aws_access_key_id = os.environ[access_key]
        self.aws_secret_access_key = os.environ[secret_key]
        self.query = query
        
    def load_conf(self, q):
        """Function for connecting and loading athena configuration"""
        try:
            self.client = boto3.client('athena', 
                              region_name = self.region_name, 
                              aws_access_key_id = self.aws_access_key_id,
                              aws_secret_access_key= self.aws_secret_access_key)
            response = self.client.start_query_execution(
                QueryString = q,
                    QueryExecutionContext={
                    'Database': self.database
                    },
                    ResultConfiguration={
                    'OutputLocation': self.s3_output,
                    }
            )
            self.filename = response['QueryExecutionId']
            print('Execution ID: ' + response['QueryExecutionId'])
            return response

        except Exception as e:
            print(e)                
  
    def run_query(self):
        """Running query by athena"""
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:              
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']['State']
                print(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                time.sleep(10)
            print('Query "{}" finished.'.format(self.query))
            
            df = self.obtain_data()
            return df
            
        except Exception as e:
            print(e)      
            
    def obtain_data(self):
        try:
            self.resource = boto3.resource('s3', 
                                  region_name = self.region_name, 
                                  aws_access_key_id = self.aws_access_key_id,
                                  aws_secret_access_key= self.aws_secret_access_key)

            response = self.resource \
            .Bucket(self.bucket) \
            .Object(key= self.folder + '/' +  self.filename + '.csv') \
            .get()
            
            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')   
        except Exception as e:
            print(e)