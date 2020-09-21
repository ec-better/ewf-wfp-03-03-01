#!/opt/anaconda/envs/env_ewf_wfp_03_03_01/bin/python

import cioppy

ciop = cioppy.Cioppy()

import urllib.parse as urlparse
import datetime
import pandas as pd
import gdal
from shapely.geometry import box


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


def tojulian(x):
    """
    Parses datetime object to julian date string.

    Args:
       datetime object 

    Returns:
        x: julian date as string YYYYJJJ
    """

    return '{}{}'.format(datetime.datetime.strptime(x, '%Y-%m-%d').timetuple().tm_year,
                                   "%03d"%datetime.datetime.strptime(x, '%Y-%m-%d').timetuple().tm_yday)



def fromjulian(x):
    """
    Parses julian date string to datetime object.

    Args:
        x: julian date as string YYYYJJJ

    Returns:
        datetime object parsed from julian date
    """

    return datetime.datetime.strptime(x, '%Y%j').date()


def get_vsi_url(enclosure, user, api_key):
    
    parsed_url = urlparse.urlparse(enclosure)

    url = '/vsicurl/%s://%s:%s@%s/api%s' % (list(parsed_url)[0],
                                            user, 
                                            api_key, 
                                            list(parsed_url)[1],
                                            list(parsed_url)[2])
    return url

def get_raster_wkt(raster):
    
    src = gdal.Open(raster)
    ulx, xres, xskew, uly, yskew, yres  = src.GetGeoTransform()
    lrx = ulx + (src.RasterXSize * xres)
    lry = uly + (src.RasterYSize * yres)

    from osgeo import ogr
    from osgeo import osr

    # Setup the source projection - you can also import from epsg, proj4...
    source = osr.SpatialReference()
    source.ImportFromWkt(src.GetProjection())

    # The target projection
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)

    # Create the transform - this can be used repeatedly
    transform = osr.CoordinateTransformation(source, target)

#    return box(transform.TransformPoint(ulx, lry)[0], 
#       transform.TransformPoint(ulx, lry)[1],
#       transform.TransformPoint(lrx, uly)[0],
#       transform.TransformPoint(lrx, uly)[1]).wkt

    return box(transform.TransformPoint(uly, lrx)[0],
       transform.TransformPoint(uly, lrx)[1],
       transform.TransformPoint(lry, ulx)[0],
       transform.TransformPoint(lry, ulx)[1]).wkt