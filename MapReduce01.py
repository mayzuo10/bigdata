# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 10:58:15 2023

@author: Lucy Zuo
"""

def map_func(x):
    """ Simple occurrence mapper """
    return (x, 1)

def shuffle(mapper_out):
    """ Organise the mapped values by key """
    data = {}
    for k, v in mapper_out:
        if k not in data: 
            data[k] = [v]
        else:
            data[k].append(v)
    return data

def reduce_func(x,y):
    """ Simple sum reducer """
    return x+y

from functools import reduce
import pandas as pd
from itertools import groupby

          
if __name__ == '__main__':
    
    """read passenger from the file"""
    passengers = pd.read_csv('./AComp_Passenger_data_no_error.csv',names=['PassengerID','FlightID','DepIATAcode','DesIATAcode','DepTime','FlightTime'])
    
    """to extract the column of PassengerID"""
    map_in = passengers['PassengerID']
    
    """mapping"""
    map_out = map(map_func, map_in)
    
    """shuffling to get the dict with PassengerID as key, and a list of 1 as value"""
    reduce_in = shuffle(map_out)
    
    reduce_out = {}
    """reducing to get a dict with Passenger ID as key and the flight times as value"""
    for key, values in reduce_in.items():
        reduce_out[key] = reduce(reduce_func, values)

    #print(reduce_out)
    
    """to get the maximum value of flight times in the dict"""
    maxTimes = max(reduce_out.values())
    
    """ turn the dict type to list type"""
    lst = list(reduce_out.items())
    
    """sort the list by the flight times from the largest to the smallest""" 
    lst.sort(key=lambda lst: lst[1], reverse=True)
    #print(lst)
    
    """group the list by flight times, then to find the all passengers with the maximum flight times"""
    glst = groupby(lst, key=lambda lst:lst[1])   
    for key, group in glst:
        if key==maxTimes:
            print(list(group))
    