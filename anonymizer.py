#!/usr/bin/env python
# coding=utf-8
from half_partition import half_partition
from utils.read_data import read_data
import sys
import pdb
# Poulis set k=25, m=2 as default!

if __name__ == '__main__':
    K = 10
    try:
        K = int(sys.argv[1])
    except:
        pass
    # read record
    data = read_data()
    # remove duplicate items
    print "Begin Half_Partition"
    result = half_partition(data, K)
    print "Finish Half_Partition!!"
