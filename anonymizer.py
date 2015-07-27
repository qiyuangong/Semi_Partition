#!/usr/bin/env python
# coding=utf-8
from semi_partition import semi_partition
from utils.read_informs_data import read_data, read_tree
import sys, copy, random


def get_result_one(att_trees, data, K=10):
    "run semi_partition for one time, with k=10"
    print "K=%d" % K
    data_back = copy.deepcopy(data)
    result, eval_result = semi_partition(att_trees, data, K)
    data = copy.deepcopy(data_back)
    print "NCP %0.2f" % eval_result[0] + "%"
    print "Running time %0.2f" % eval_result[1] + "seconds"


def get_result_K(att_trees, data):
    """
    change K, whle fixing QD and size of dataset
    """
    data_back = copy.deepcopy(data)
    for K in range(5, 55, 5):
        print '#' * 30
        print "K=%d" % K
        result, eval_result = semi_partition(att_trees, data, K)
        data = copy.deepcopy(data_back)
        print "NCP %0.2f" % eval_result[0] + "%"
        print "Running time %0.2f" % eval_result[1] + "seconds"


def get_result_dataset(att_trees, data, K=10, n=10):
    """
    fix k and QI, while changing size of dataset
    n is the proportion nubmber.
    """
    data_back = copy.deepcopy(data)
    length = len(data_back)
    print "K=%d" % K
    joint = 5000
    h = length / joint
    if length % joint == 0:
        h += 1
    for i in range(1, h + 1):
        pos = i * joint
        ncp = rtime = 0
        if pos > length:
            continue
        print '#' * 30
        print "size of dataset %d" % pos
        for j in range(n):
            temp = random.sample(data, pos)
            result, eval_result = semi_partition(att_trees, temp, K)
            ncp += eval_result[0]
            rtime += eval_result[1]
            data = copy.deepcopy(data_back)
            # save_to_file((att_trees, temp, result, K, L))
        ncp /= n
        rtime /= n
        print "Average NCP %0.2f" % ncp + "%"
        print "Running time %0.2f" % rtime + "seconds"
        print '#' * 30


def get_result_QI(att_trees, data, K=10):
    pass


if __name__ == '__main__':
    flag = ''
    len_argv = len(sys.argv)
    try:
        flag = sys.argv[1]
    except:
        pass
    K = 10
    att_trees = read_tree()
    # read record
    data = read_data()
    if flag == 'k':
        get_result_K(att_trees, data)
    if flag == 'qi':
        get_result_QI(att_trees, data)
    elif flag == 'data':
        get_result_dataset(att_trees, data)
    elif flag == 'one':
        if len_argv > 2:
            K = int(sys.argv[2])
            get_result_one(att_trees, data, K)
        else:
            get_result_one(att_trees, data)
    elif flag == '':
        get_result_one(att_trees, data)
    else:
        print "Usage: python anonymizer [k | qi |data | one]"
    # anonymized dataset is stored in result
    print "Finish Semi_Partition!!"
