"""
__author__ = "Giovanni Zambotti"
__copyright__ = ""
__credits__ = ["Giovanni Zambotti"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Giovanni Zambotti"
__email__ = "g.zambotti@gmail.com"
__status__ = "Production"
"""

import arcgis
from arcgis.gis import GIS
from arcgis import features
from arcgis import geoanalytics
from arcgis.features import SpatialDataFrame
#from getpass import getpass #to accept passwords in an interactive fashion
import pandas as pd
import json
import requests
from pandas.io.json import json_normalize
from copy import deepcopy
import numpy as np
import datetime
from datetime import datetime
import time, os

"""
NOTE: All the five dots ..... required the correct informations
"""
gis = GIS(".....", "......", ".....", verify_cert=False)

fc_id = "3b43c19cf21d42eabea83a9a99977472"

def bb():    
    try:
        # script to update the status of the blue bikes
        r = requests.get('https://gbfs.bluebikes.com/gbfs/en/station_status.json')
        r = r.json()['data']['stations']
        df = pd.DataFrame.from_dict(json_normalize(r), orient='columns')
        df = df.rename(columns={'last_reported':'last_reported','station_id':'description', 'num_bikes_available':'bikes_available',
                                'num_docks_available':'docks_available'})

        
        bb_table = df[['last_reported','description', 'bikes_available', 'docks_available']]
        
        bb_features = gis.content.get(fc_id).layers[7]
        bb_features
        bb_fset = SpatialDataFrame.from_layer(bb_features)
        bb_fset = bb_features.query(where="category_subtype = 'Blue Bikes'") #querying without any conditions returns all the features    

        overlap_rows = pd.merge(left = bb_fset.sdf, right = bb_table, how='inner', on = 'description')
        
        #print (overlap_rows.head())
        features_for_update = [] #list containing corrected features
        all_features = bb_fset.features

        # inspect one of the features
        #print (all_features[2])

        # update all features that were joined
        for root_id in overlap_rows['description']:    
            # get the feature to be updated
            original_feature = [f for f in all_features if f.attributes['description'] == root_id][0]

            feature_to_be_updated = deepcopy(original_feature)        

            # get the matching row from csv
            matching_row = bb_table.where(bb_table.description == root_id).dropna()
            #print('snr' + tablenumber + '', 'snr' + tablenumber + 'average', float(matching_row['snrval'].values))
            
            timestamp = matching_row['last_reported'].values[0]
            
            #print (str(datetime.fromtimestamp(timestamp)))
            feature_to_be_updated.attributes['use_type'] = matching_row['bikes_available'].values[0]
            feature_to_be_updated.attributes['source_name'] = matching_row['docks_available'].values[0]
            #feature_to_be_updated.attributes['source_type'] = str(datetime.fromtimestamp(matching_row['last_reported'].values[0]))
            
            feature_to_be_updated.attributes['source_type'] = str(datetime.fromtimestamp(matching_row['last_reported'].values[0]).strftime("%I:%M %p"))

            features_for_update.append(feature_to_be_updated)
        #print (features_for_update)
        bb_features.edit_features(updates= features_for_update)
    except:
        print("Something went wrong")
        os._exit(0)
        
if __name__ == '__main__':
    bb()
