"""
main module of Semi_Partition
"""

# !/usr/bin/env python
# coding=utf-8


import pdb
import random
from models.numrange import NumRange
from models.gentree import GenTree
from utils.utility import cmp_str
from mondrian import Partition
from mondrian import get_normalized_width
from mondrian import check_splitable
from mondrian import split_numeric_value
from mondrian import choose_dimension
import time


__DEBUG = False
QI_LEN = 10
GL_K = 0
RESULT = []
ATT_TREES = []
QI_RANGE = []
IS_CAT = []
# TODO Relax numeric partition


def split_categoric(partition, dim, pwidth, pmiddle):
    """
    split categorical attribute using generalization hierarchy
    """
    sub_partitions = []
    # categoric attributes
    if partition.middle[dim] != '*':
        splitVal = ATT_TREES[dim][partition.middle[dim]]
    else:
        splitVal = ATT_TREES[dim]['*']
    sub_node = [t for t in splitVal.child]
    sub_groups = []
    for i in range(len(sub_node)):
        sub_groups.append([])
    if len(sub_groups) == 0:
        # split is not necessary
        return []
    for temp in partition.member:
        qid_value = temp[dim]
        for i, node in enumerate(sub_node):
            try:
                node.cover[qid_value]
                sub_groups[i].append(temp)
                break
            except KeyError:
                continue
        else:
            print "Generalization hierarchy error!"
    for i, p in enumerate(sub_groups):
        if len(p) == 0:
            continue
        wtemp = pwidth[:]
        mtemp = pmiddle[:]
        wtemp[dim] = len(sub_node[i])
        mtemp[dim] = sub_node[i].value
        sub_partitions.append(Partition(p, wtemp, mtemp))
    return sub_partitions


def split_partition(partition, dim):
    """
    split partition and distribute records to different sub-partitions
    """
    pwidth = partition.width
    pmiddle = partition.middle
    if IS_CAT[dim] is False:
        return split_numeric(partition, dim, pwidth, pmiddle)
    else:
        return split_categoric(partition, dim, pwidth, pmiddle)


def balance_partition(sub_partitions, leftover):
    """
    balance partitions:
    Step 1: For partitions with less than k records, merge them to leftover partition.
    Step 2: If leftover partition has less than k records, then move some records
    from partitions with more than k records.
    Step 3: After Step 2, if the leftover partition does not satisfy
    k-anonymity, then merge a partitions with k records to the leftover partition.
    Final: Backtrace leftover partition to the partent node.
    """
    if len(sub_partitions) <= 1:
        # split failure
        return
    extra = 0
    check_list = []
    for sub_p in sub_partitions[:]:
        record_set = sub_p.member
        if len(record_set) < GL_K:
            leftover.member.extend(record_set)
            sub_partitions.remove(sub_p)
        else:
            extra += len(record_set) - GL_K
            check_list.append(sub_p)
    # there is no record to balance
    if len(leftover) == 0:
        return
    ls = len(leftover)
    if ls < GL_K:
        need_for_leftover = GL_K - ls
        if need_for_leftover > extra:
            min_p = 0
            min_size = len(check_list[0])
            for i, sub_p in enumerate(check_list):
                if len(sub_p) < min_size:
                    min_size = len(sub_p)
                    min_p = i
            sub_p = sub_partitions.pop(min_p)
            leftover.member.extend(sub_p.member)
        else:
            while need_for_leftover > 0:
                check_list = [t for t in sub_partitions if len(t) > GL_K]
                for sub_p in check_list:
                    if need_for_leftover > 0:
                        t = sub_p.member.pop(random.randrange(len(sub_p)))
                        leftover.member.append(t)
                        need_for_leftover -= 1
    sub_partitions.append(leftover)


def anonymize(partition):
    """
    Main procedure of Half_Partition.
    recursively partition groups until not allowable.
    """
    if check_splitable(partition) is False:
        RESULT.append(partition)
        return
    # leftover contains all records from subPartitons smaller than k
    # So the GH of leftover is the same as Parent.
    leftover = Partition([], partition.width, partition.middle)
    # Choose dim
    dim = choose_dimension(partition)
    if dim == -1:
        print "Error: dim=-1"
        pdb.set_trace()
    # leftover.allow[dim] = 0
    # balance sub-partitions
    sub_partitions = split_partition(partition, dim)
    if len(sub_partitions) == 0:
        partition.allow[dim] = 0
        anonymize(partition)
    else:
        if len(sub_partitions) > 1:
            balance_partition(sub_partitions, leftover)
            if len(sub_partitions) == 1:
                partition.allow[dim] = 0
                anonymize(partition)
                return
        for sub_p in sub_partitions:
            anonymize(sub_p)


def init(att_trees, data, K, QI_num=-1):
    """
    reset all global variables
    """
    global GL_K, RESULT, QI_LEN, ATT_TREES, QI_RANGE, IS_CAT
    ATT_TREES = att_trees
    for t in att_trees:
        if isinstance(t, NumRange):
            IS_CAT.append(False)
        else:
            IS_CAT.append(True)
    if QI_num <= 0:
        QI_LEN = len(data[0]) - 1
    else:
        QI_LEN = QI_num
    GL_K = K
    RESULT = []
    QI_RANGE = []


def semi_partition(att_trees, data, K, QI_num=-1):
    """
    Semi_Partition for k-anonymity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    init(att_trees, data, K, QI_num)
    result = []
    middle = []
    wtemp = []
    for i in range(QI_LEN):
        if IS_CAT[i] is False:
            QI_RANGE.append(ATT_TREES[i].range)
            wtemp.append((0, len(ATT_TREES[i].sort_value) - 1))
            middle.append(ATT_TREES[i].value)
        else:
            QI_RANGE.append(len(ATT_TREES[i]['*']))
            wtemp.append(len(ATT_TREES[i]['*']))
            middle.append('*')
    whole_partition = Partition(data, wtemp, middle)
    start_time = time.time()
    anonymize(whole_partition)
    rtime = float(time.time() - start_time)
    ncp = 0.0
    for partition in RESULT:
        r_ncp = 0.0
        for i in range(QI_LEN):
            r_ncp += get_normalized_width(partition, i)
        temp = partition.middle
        for i in range(len(partition)):
            result.append(temp[:])
        r_ncp *= len(partition)
        ncp += r_ncp
    # covert to NCP percentage
    ncp /= QI_LEN
    ncp /= len(data)
    ncp *= 100
    if len(result) != len(data):
        print "Losing records during anonymization!!"
        pdb.set_trace()
    if __DEBUG:
        print "K=%d" % K
        print "size of partitions"
        print len(RESULT)
        temp = [len(t) for t in RESULT]
        print sorted(temp)
        print "NCP = %.2f %%" % ncp
    return (result, (ncp, rtime))
