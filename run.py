"""
Running Covid 19 etl job

"""
import argparse
import logging
import logging.config
import yaml
from transformers.ETL import ETL
from config.conf import EtlConf,ConnectionConf
from common.queryathena import QueryAthena


def main():
    """
    Main function for running covid 19 etl job

    """
    # Parsing YML file
    parser = argparse.ArgumentParser(description='Run the Covid19 ETL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')


    config = 'C:/Emir/Projects/Covid 19 Project/covid19/config/covid19_etl_config.yaml'
    config = yaml.safe_load(open(file=config,encoding='utf8'))

    log_config = config['logging']
    logging.config.dictConfig(log_config)

    connection_config=ConnectionConf(**config['connectionconf'])
    etl_config=EtlConf(**config['etlconf'])

    etl=ETL(queryathena=QueryAthena,connectionconf=connection_config,etlconf=etl_config)

    etl.Extract()
    etl.Transform()
    etl.Load()

if __name__=='__main__':
    main()