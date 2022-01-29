import geopandas
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

pd.options.mode.chained_assignment = None

def outlier_detection(data: pd.DataFrame)->list:
    """
    this function identify ouliers looking at the average speed

    Parameters
    ----------
    data : pd.DataFrame
        [description]

    Returns
    -------
    list
        [description]
    """

    index_to_cut = [] 
    number_more_than = 1

    data_copy = data.copy()

    while number_more_than>0 :

        if len(index_to_cut)>0:
            data = data_copy.query(""" ~ index.isin(@index_to_cut) """)

        # compute distance among each point
        data[['timestamp_shifted','geometry_shifted','occupation_shift']] = data[['timestamp','geometry','occupation']].shift()
        data = data[1:]
        data['distance'] = data.apply(lambda x: x.geometry.distance(x.geometry_shifted),axis=1)
        data['time_delta'] = data.apply(lambda x: x.timestamp - x.timestamp_shifted, axis=1)
        data['time_delta_s'] = data.time_delta.astype('timedelta64[s]')

        # Computer average speed
        data['speed_kmh'] = data.eval(" 3.6*distance/(time_delta_s) ")

        # Metrics
        perc_less_than = data.query(" speed_kmh < 200").shape[0]/data.shape[0]
        perc_more_than = data.query(" speed_kmh > 200").shape[0]/data.shape[0]
        number_more_than = data.query(" speed_kmh > 200").shape[0]

        logging.info(f"percentage records above, below, #record above the threshold = ({100*round(perc_more_than,5)} %, {100*round(perc_less_than,5)} %, {round(number_more_than,3)})")

        index_to_cut = np.append(index_to_cut, data.query(" speed_kmh > 200").index.values)

    logging.info(f"number of outliers = {len(index_to_cut)}")
    logging.info(f"percentage of outliers over the total records = {100*round(len(index_to_cut)/data.shape[0],4)} %")
    
    return index_to_cut, data

counter=0
def travels_counter(x):
    global counter
    if x != 0:
        counter +=1
    return counter