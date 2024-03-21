"""
Module of etl job

"""
from io import StringIO
import logging
import pandas as pd
import boto3
from config.conf import ConnectionConf
from config.conf import EtlConf
from common.queryathena import QueryAthena

class ETL:
    """
    Class of etl job

    """
    def __init__(self,queryathena:QueryAthena,connectionconf:ConnectionConf,etlconf:EtlConf):
        self.query_athena=queryathena
        self.connection_args=connectionconf
        self.etl_args=etlconf
        self.nytimes_data_in_usa_us_contries=None
        self.rearc_covid19_testing_data_states_daily=None
        self.rearc_usa_hostipal_beds=None
        self.enigma_jhud=None
        self.fact_covid=None
        self.dim_date=None
        self.dim_hospital=None
        self.dim_region=None
        self._logger = logging.getLogger(__name__)


    def Extract(self):
        """
        Extracting data with Athena Query
        """
        self._logger.info('Extracting nytimes_data_in_usa_us_contries file is started...')
        self.nytimes_data_in_usa_us_contries=self.query_athena(database=self.connection_args.database,\
                                                          folder=self.connection_args.folder,\
                                                            bucket=self.connection_args.bucket,\
                                                                region=self.connection_args.region,\
                                                                    access_key=self.connection_args.access_key,\
                                                                        secret_key=self.connection_args.secret_key,\
                                                                            query=self.connection_args.query['nytimes_data_in_usa_us_contries']).run_query()
        self._logger.info('Extracting rearc_covid19_testing_data_states_daily file is started...')
        self.rearc_covid19_testing_data_states_daily=self.query_athena(database=self.connection_args.database,\
                                                            folder=self.connection_args.folder,\
                                                            bucket=self.connection_args.bucket,\
                                                                region=self.connection_args.region,\
                                                                    access_key=self.connection_args.access_key,\
                                                                        secret_key=self.connection_args.secret_key,\
                                                                            query=self.connection_args.query['rearc_covid19_testing_data_states_daily']).run_query()
        self._logger.info('Extracting rearc_usa_hostipal_beds file is started...')
        self.rearc_usa_hostipal_beds=self.query_athena(database=self.connection_args.database,\
                                                    folder=self.connection_args.folder,\
                                                    bucket=self.connection_args.bucket,\
                                                        region=self.connection_args.region,\
                                                            access_key=self.connection_args.access_key,\
                                                                secret_key=self.connection_args.secret_key,\
                                                                    query=self.connection_args.query['rearc_usa_hostipal_beds']).run_query()
        self._logger.info('Extracting enigma_jhud file is started...')
        self.enigma_jhud=self.query_athena(database=self.connection_args.database,\
                                    folder=self.connection_args.folder,\
                                    bucket=self.connection_args.bucket,\
                                        region=self.connection_args.region,\
                                            access_key=self.connection_args.access_key,\
                                                secret_key=self.connection_args.secret_key,\
                                                    query=self.connection_args.query['enigma_jhud']).run_query()
        self._logger.info('Extracting of all files is finished...')
    def Transform(self):
        """
        Transforms data
        """
        self._logger.info('Data transformation is started...')
        fact_covid_1=self.enigma_jhud[self.etl_args.fact_covid_1_columns]
        fact_covid_2=self.rearc_covid19_testing_data_states_daily[self.etl_args.fact_covid_2_columns]
        self.fact_covid=pd.merge(fact_covid_1,fact_covid_2,on='fips',how='inner')

        dim_region_1=self.enigma_jhud[self.etl_args.dim_region_1_columns]
        dim_region_2=self.nytimes_data_in_usa_us_contries[self.etl_args.dim_region_2_columns]
        self.dim_region=pd.merge(dim_region_1,dim_region_2,on='fips',how='inner')

        self.dim_hospital=self.rearc_usa_hostipal_beds[self.etl_args.dim_hospital]

        self.dim_date=self.rearc_covid19_testing_data_states_daily[self.etl_args.dim_date]
        self.dim_date['date']=pd.to_datetime(self.dim_date['date'],format='%Y%m%d')
        self.dim_date['year']=self.dim_date['date'].dt.year
        self.dim_date['month']=self.dim_date['date'].dt.month
        self.dim_date['day_of_week']=self.dim_date['date'].dt.dayofweek
        self._logger.info('Data transformation is finished...')

    def Load(self):
        """
        Loading data to S3 Bucket
        """
        self._logger.info('Loading factCovid file is started...')
        csv_buffer=StringIO()
        self.fact_covid.to_csv(csv_buffer)
        s3_resource=boto3.resource('s3')
        s3_resource.Object(self.connection_args.bucket,'output/tables/factCovid.csv').put(Body=csv_buffer.getvalue())
        self._logger.info('Loading factCovid file is finished...')

        self._logger.info('Loading dimDate file is started...')
        csv_buffer=StringIO()
        self.dim_date.to_csv(csv_buffer)
        s3_resource=boto3.resource('s3')
        s3_resource.Object(self.connection_args.bucket,'output/tables/dimDate.csv').put(Body=csv_buffer.getvalue())
        self._logger.info('Loading dimDate file is finished...')

        self._logger.info('Loading dimHospital file is started...')
        csv_buffer=StringIO()
        self.dim_hospital.to_csv(csv_buffer)
        s3_resource=boto3.resource('s3')
        s3_resource.Object(self.connection_args.bucket,'output/tables/dimHospital.csv').put(Body=csv_buffer.getvalue())
        self._logger.info('Loading dimHospital file is finished...')

        self._logger.info('Loading dimRegion file is started...')
        csv_buffer=StringIO()
        self.dim_region.to_csv(csv_buffer)
        s3_resource=boto3.resource('s3')
        s3_resource.Object(self.connection_args.bucket,'output/tables/dimRegion.csv').put(Body=csv_buffer.getvalue())
        self._logger.info('Loading dimRegion file is finished...')