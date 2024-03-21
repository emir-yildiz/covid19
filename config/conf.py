from typing import NamedTuple


class ConnectionConf(NamedTuple):
    """
    access_key: Access key for AWS connection
    secret_key: Secret key for AWS connection
    region: Region of s3 bucket
    bucket: Output bucket for Athena Query
    folder: Output folder for Athena Query
    database: Database of Athena Query
    query: Query for Athena Database
    """

    access_key: str
    secret_key: str
    region: str
    bucket: str
    folder: str
    database: str
    query: dict

class EtlConf(NamedTuple):
    """
    fact_covid_1_columns: Column List
    fact_covid_2_columns: Column List
    dim_region_1_columns: Column List
    dim_region_2_columns: Column List
    dim_hospital: Column List
    dim_date: Column List

    """
    fact_covid_1_columns: list
    fact_covid_2_columns: list
    dim_region_1_columns: list
    dim_region_2_columns: list
    dim_hospital: list
    dim_date: list


