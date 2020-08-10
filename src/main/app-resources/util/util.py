#!/opt/anaconda/envs/env_ewf_wfp_03_03_01/bin/python

import cioppy

ciop = cioppy.Cioppy()
import datetime
import pandas as pd
def log_input(reference):
    """
    Just logs the input reference, using the ciop.log function
    """

    ciop.log('INFO', 'processing input: ' + reference)
    
def pass_next_node(input):
    """
    Pass the input reference to the next node as is, without storing it on HDFS
    """

    ciop.publish(input, mode='silent')

    
def name_date_from_enclosure(row):
    series = dict()
    series['name']=(row['enclosure'].split('/')[-1]).split('.')[0]
    print(series['name'])
    series['day']=series['name'][-26:-18]
    series['jday'] = '{}{}'.format(datetime.datetime.strptime(series['day'], '%Y%m%d').timetuple().tm_year,
                                   "%03d"%datetime.datetime.strptime(series['day'], '%Y%m%d').timetuple().tm_yday)

    return pd.Series(series)