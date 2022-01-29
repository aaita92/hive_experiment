"""
This comdul contains a simple script to preprocess raw data and save them on the hdfs.
  
"""

# =============================================================================
# Module attributes

__docformat__ = 'NumPy'
# License type (e.g. 'GPL', 'MIT', 'Proprietary', ...)
__license__ = 'Proprietary'
# Status ('Prototype', 'Development' or 'Pre-production')
__status__ = 'Development'
# Version (0.0.x = 'Prototype', 0.x.x = 'Development', x.x.x = 'Pre-production)
__version__ = '0.1.0'
# Authors (e.g. code writers)
__author__ = ('Antonello Aita <antonello.aita@gmail.com>')
# Maintainer
__maintainer__ ="Antonello Aita" 
# Email of the maintainer
__email__ = 'antonello.aita@gmail.com'
# =============================================================================
# Import modules
# =============================================================================
# Import general purpose module(s)
import geopandas
import pandas as pd
import geoplot
import matplotlib.pyplot as plt
import numpy as np
import logging
import os
import sys
import utils

# Import Custom moduls


#===========================================================================================#
# Logger setting 
#===========================================================================================#
file_handler = logging.FileHandler(filename='logfile.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(format='%(asctime)s | %(message)s', level=logging.INFO, handlers=handlers)

#===========================================================================================#
# Uploader engine
#===========================================================================================#
logging.info("===============================START======================================")

path = '...'

for file in os.listdir(path)[1:]:
    
    filename = os.path.join(path,file)


    logging.info("======= Filename: "+str(file)+" ========")

    #0) Read file
    data_raw = pd.read_csv(filename,names=['lat','lon','occupation','timestamp'],sep=' ')

    # Timestamp handling
    data_raw['timestamp'] = data_raw.timestamp.apply(lambda x: pd.to_datetime(x,unit='s'))
    data_raw['date'] = data_raw.timestamp.dt.date
    data_raw['hour'] = data_raw.timestamp.dt.hour


    data = data_raw.copy()

   # Geospatial reading
    data = geopandas.GeoDataFrame(data, geometry=geopandas.points_from_xy(data.lon, data.lat), crs='EPSG:4326')
    data.geometry = data.geometry.to_crs('EPSG:7131')


    #1) Outlier detection

    index_to_cut, data = utils.outlier_detection(data=data)

    #2) Report outliers on data
    # Move the outliers on the original dataframe
    data_raw['excluded_data'] = 0
    data_raw.loc[index_to_cut,'excluded_data'] = 1
    
    #3) Passenger status
    data['passenger_status'] = data.apply(lambda x: x.occupation-x.occupation_shift,axis=1)
    # Set the counter to 0 in order to count the 
    data['counter_travels'] = data.passenger_status.apply(utils.travels_counter)

    data_ready = data_raw.merge(data[['passenger_status','counter_travels']], how='left', left_index=True, right_index=True)

    #4) [QUALITY]  check numeber of charging/discharging
    logging.info("Charging/discargin count =  "+str(data.query(" passenger_status !=0 ").passenger_status.value_counts().to_json()))    

    #5) Counter travels
    travels = data.groupby('counter_travels').agg({'passenger_status':'first','distance':'sum','time_delta_s':'sum'})    
    travels = travels.reset_index()
    logging.info(f"Travels counter = {travels.shape[0]} ")


    #6) Exporting data
    file = file.split('.')[0]

    # Filename on local 
    travels_file = "Hive/hive_stage/"+"travels_"+file+".csv"
    data_ready_file = "Hive/hive_stage/"+"ready_"+file+".csv"

    # Filename and path on the Hive docker
    travels_file_hive = "../hive_stage/"+"travels_"+file+".csv"
    data_ready_file_hive = "../hive_stage/"+"ready_"+file+".csv"

    # Save on local
    logging.info('Local saving')
    travels.fillna(0).to_csv(travels_file, header=False, index=False)
    data_ready.fillna(0).to_csv(data_ready_file, header=False, index=False)
    logging.info("Local saving - Done")

    # Put data on the hadoop filesystem
    command_travel = f"docker exec -it hive-server hadoop fs -put -f {travels_file_hive} hdfs://namenode:8020/user/hive/warehouse/processed_data.db/travels"
    command_data_ready = f"docker exec -it hive-server hadoop fs -put -f {data_ready_file_hive} hdfs://namenode:8020/user/hive/warehouse/processed_data.db/data_ready"

    logging.info("Hadoop uploading")
    os.system(command_data_ready)
    os.system(command_travel)
    logging.info("Hadoop uploading - Done")

    # Delete file from local
    logging.info("File deleting")
    os.remove(travels_file)
    os.remove(data_ready_file)
    logging.info("File deleting- Done")