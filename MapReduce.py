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


          
if __name__ == '__main__':

    map_in = ['to', 'be', 'or', 'not', 'to', 'be','to','be', 'or','not']
    
    map_out = map(map_func, map_in)

    reduce_in = shuffle(map_out)
    
    reduce_out = {}
    
    for key, values in reduce_in.items():
        reduce_out[key] = reduce(reduce_func, values)
    
    print(reduce_out)