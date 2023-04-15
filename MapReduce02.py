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
import multiprocessing as mp
from itertools import groupby

"""reduce function to adapt to the requirement of only one parameter in pool.map"""
def reduce_func1(x):
    reduce_out = {}
    i=1
    for x0 in x:
        if(i%2==1):
            key=x0
        if(i%2==0):
            lst = x0
            reduce_out[key] = reduce(reduce_func, lst)     #to carry reducing
        i=i+1
    return reduce_out
          
if __name__ == '__main__':
    
    """read passenger from the file"""
    passengers = pd.read_csv('./AComp_Passenger_data_no_error.csv',names=['PassengerID','FlightID','DepIATAcode','DesIATAcode','DepTime','FlightTime'])

    """to extract the column of PassengerID"""
    map_in = passengers['PassengerID']
     
    """parallel implementation on  multiple processors """
    with mp.Pool(processes=mp.cpu_count()) as pool:
        #print(mp.cpu_count())
        #print(int(len(map_in)/mp.cpu_count())+1)
        
        """deploy map_func on multiple processeors"""
        map_out = pool.map(map_func, map_in, chunksize=int(len(map_in)/mp.cpu_count()))

        """shuffling to get the dict with PassengerID as key, and a list of 1 as value""" 
        reduce_in = shuffle(map_out)
        #print(reduce_in)
        
        """reducing to get a list of a tuple (Passenger ID, flight times)"""
        reduce_out = pool.map(reduce_func1, reduce_in.items(), chunksize=int(len(reduce_in.keys())/mp.cpu_count()))
      
    #print(reduce_out)
    #print(len(reduce_out))
    """to get the dict reduce_out1 from the tuple reduce_out"""
    reduce_out1 = {}
    for elem in reduce_out:
        for k, v in elem.items():
            reduce_out1[k]=v
    #print(reduce_out1)
    
    """to get the maximum value of flight times"""
    maxTimes = max(reduce_out1.values())
    
    """ turn the dict type to list type"""
    lst = list(reduce_out1.items())

    """sort the list by the flight times from the largest to the smallest""" 
    lst.sort(key=lambda lst: lst[1], reverse=True)
    #print(lst)
    
    """group the list by flight times, then to find all passengers with the maximum flight times"""
    glst = groupby(lst, key=lambda lst:lst[1])  
    for key, group in glst:
        if key==maxTimes:
            print(list(group))
            