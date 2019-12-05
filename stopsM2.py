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
from arcgis.features import GeoAccessor, GeoSeriesAccessor
#from arcgis import geoanalytics
#from arcgis.features import SpatialDataFrame
#from getpass import getpass #to accept passwords in an interactive fashion
import pandas as pd
import json
import requests
from pandas.io.json import json_normalize
from copy import deepcopy
import numpy as np
import datetime
from datetime import datetime
import time

gis = GIS(".....", ".....", ".....", verify_cert=False)

# routes HU feed
fmt = "%Y-%m-%dT%H:%M:%S"        
# feature class id
fc_id = '3b43c19cf21d42eabea83a9a99977472'


def routefeed(route_id_val):
    # routes feed    
    key = "....."
    headers = {'X-Mashape-Key' : key,'Accept' : 'application/json'}    
    r = requests.get('https://transloc-api-1-2.p.rapidapi.com/routes.json?callback=call&agencies=64', headers=headers)
    routes = r.json()['data']
    df_routes = pd.DataFrame.from_dict(json_normalize(routes['64']), orient='columns')
    
    for index, row in df_routes.iterrows():
        #print(row['long_name'],row['route_id'])
        if(row['route_id'] == route_id_val):
            return row['long_name']

def stopsM2():
    try:
        key = "....."
        headers = {'X-Mashape-Key' : key,'Accept' : 'application/json'}
        r = requests.get('https://transloc-api-1-2.p.mashape.com/stops.json?agencies=64&callback=call', headers=headers)
        stops = r.json()['data']

        m2_stops = pd.DataFrame.from_dict(json_normalize(stops), orient='columns')

        # M2 arrival-estimates feed
        key = "....."
        headers = {'X-Mashape-Key' : key,'Accept' : 'application/json'}
        r = requests.get('https://transloc-api-1-2.p.rapidapi.com/arrival-estimates.json?callback=call&agencies=64', headers=headers)
        arrival = r.json()['data']
        m2_arrival = pd.DataFrame.from_dict(json_normalize(arrival), orient='columns')

        if m2_arrival.empty is True:
            print ("No Arrival")
        else:    
            m2_stops_merge = pd.merge(left = m2_stops, right = m2_arrival, how='left', on='stop_id')    
            m2_stops_merge = m2_stops_merge.rename(columns={'code':'code','name':'name','stop_id':'stop_id','arrivals':'arrivals'})    
            df1 = m2_stops_merge[['code','name','stop_id','arrivals']]    
            d = []
            t2 = time.strftime(fmt)
            for index, row in df1.iterrows():            
                if type(row['arrivals']) == list:
                    row_z = row['arrivals'][:2]                
                    for x in range(0, len(row_z)):                    
                        if(len(row_z) == 2):
                            predicted_time = []
                            for x in range(0, len(row_z)):        
                                t1 = time.strftime(row_z[x]['arrival_at'][:-6])
                                tdelta = datetime.strptime(t1, fmt) - datetime.strptime(t2, fmt)
                                if(int(tdelta.total_seconds()/60) > 0):
                                    predicted_time.append(int(tdelta.total_seconds()/60))
                                else:
                                    print(int(tdelta.total_seconds()/60))    	
                            #print (x, len(row_z), row['code'], row['name'], row_z[x]['route_id'], t1, ' & '.join(map(str, predicted_time)))
                            d.append([routefeed(row['arrivals'][x]['route_id']), row['code'], ' & '.join(map(str, predicted_time)).replace('0 & ',''), row['stop_id'], row['name']])
                        else:
                            #print (len(row_z), row['code'])                                            
                            #print(row['code'],len(row['arrivals']),row['arrivals'])
                            for x in range(0, len(row['arrivals'])):
                                #print (x, row['arrivals'][x]['route_id'])
                                t1 = time.strftime(row['arrivals'][x]['arrival_at'][:-6])             
                                tdelta = datetime.strptime(t1, fmt) - datetime.strptime(t2, fmt)
                                #print (routefeed(row['arrivals'][x]['route_id']), int(tdelta.total_seconds()/60), row['stop_id'], row['name'])            
                                d.append([routefeed(row['arrivals'][x]['route_id']), row['code'], int(tdelta.total_seconds()/60), row['stop_id'], row['name']])
                            
                else:            
                    #print ("---", "Text: HARV " + str(row['code']) + " to 41411",  str(row['stop_id']), row['name'] )
                    d.append(['' , row['code'],'', row['stop_id'], row['name']])

            a = pd.DataFrame(d, columns=['route_id','code','time','stop_id','name'])
            
            stops_feed = pd.DataFrame(a.groupby(['route_id','stop_id', 'code'],as_index=False)['time'].agg(lambda x: list(x)))    
            
            #display(stops_feed)
            stops_features = gis.content.get(fc_id).layers[7]    
            #print (stops_features)
            stops_fset = stops_features.query(where="category_subtype = 'M2 Shuttle Stops'") #querying without any conditions returns all the features
            #display(stops_fset.df)
            overlap_rows = pd.merge(left = stops_fset.df, right = stops_feed, how='inner', left_on='source_name', right_on='stop_id')
            #display(overlap_rows)
            features_for_update = [] #list containing corrected features
            all_features = stops_fset.features
            # update all features that were joined
            for root_id in overlap_rows['source_name']:            
                # get the feature to be updated
                original_feature = [f for f in all_features if f.attributes['source_name'] == root_id][0]
                feature_to_be_updated = deepcopy(original_feature)
                # get the matching row from csv
                matching_row = stops_feed.where(stops_feed.stop_id == root_id).dropna() 
                print (root_id, matching_row['time'].values[0][0])
                
                feature_to_be_updated.attributes['description'] = matching_row['time'].values[0][0]
                feature_to_be_updated.attributes['name_long'] = matching_row['route_id'].values[0]
                feature_to_be_updated.attributes['site_name'] = matching_row['code'].values[0]
                
                features_for_update.append(feature_to_be_updated)
            ##print (features_for_update)
            stops_features.edit_features(updates= features_for_update)
    except:        
        print("An exception occurred")

if __name__ == '__main__':
    stopsM2()
