#!/usr/bin/env python
# coding=utf-8
from semi_partition import semi_partition
from utils.read_data import read_data, read_tree
import sys
# Poulis set k=25 as default!

if __name__ == '__main__':
    K = 10
    try:
        K = int(sys.argv[1])
    except:
        pass
    att_trees = read_tree()
    # read record
    data = read_data()
    # remove duplicate items
    print "Begin Half_Partition"
    result = semi_partition(att_trees, data, K)
    print "Finish Half_Partition!!"
