"""
main module of Mondrian
"""

# !/usr/bin/env python
# coding=utf-8


import pdb
import random
from models.numrange import NumRange
from models.gentree import GenTree
from utils.utility import cmp_str
import time


__DEBUG = False
QI_LEN = 10
GL_K = 0
RESULT = []
ATT_TREES = []
QI_RANGE = []
IS_CAT = []


class Partition(object):

    """Class for Group, which is used to keep records
    Store tree node in instances.
    self.member: records in group
    self.width: width of this partition on each domain. For categoric attribute, it equal
    the number of leaf node, for numeric attribute, it equal to number range
    self.middle: save the generalization result of this partition
    self.allow: 0 donate that not allow to split, 1 donate can be split
    """

    def __init__(self, data, width, middle):
        """
        initialize with data, width and middle
        """
        self.member = list(data)
        self.width = list(width)
        self.middle = list(middle)
        self.allow = [1] * QI_LEN

    def __len__(self):
        """
        return the number of records in partition
        """
        return len(self.member)


def get_normalized_width(partition, index):
    """
    return Normalized width of partition
    similar to NCP
    """
    if IS_CAT[index] is False:
        low = partition.width[index][0]
        high = partition.width[index][1]
        width = float(ATT_TREES[index].sort_value[high]) - float(ATT_TREES[index].sort_value[low])
    else:
        width = partition.width[index]
    return width * 1.0 / QI_RANGE[index]


def choose_dimension(partition):
    """
    chooss dim with largest normlized Width
    return dim index.
    """
    max_width = -1
    max_dim = -1
    for i in range(QI_LEN):
        if partition.allow[i] == 0:
            continue
        normWidth = get_normalized_width(partition, i)
        if normWidth > max_width:
            max_width = normWidth
            max_dim = i
    if max_width > 1:
        print "Error: max_width > 1"
        pdb.set_trace()
    if max_dim == -1:
        print "cannot find the max dim"
        pdb.set_trace()
    return max_dim


def frequency_set(partition, dim):
    """
    get the frequency_set of partition on dim
    return dict{key: str values, values: count}
    """
    frequency = {}
    for record in partition.member:
        try:
            frequency[record[dim]] += 1
        except KeyError:
            frequency[record[dim]] = 1
    return frequency


def find_median(partition, dim):
    """
    find the middle of the partition
    return splitVal
    """
    frequency = frequency_set(partition, dim)
    splitVal = ''
    value_list = frequency.keys()
    value_list.sort(cmp=cmp_str)
    total = sum(frequency.values())
    middle = total / 2
    if middle < GL_K or len(value_list) <= 1:
        try:
            return ('', '', value_list[0], value_list[-1])
        except IndexError:
            return ('', '', '', '')
    index = 0
    split_index = 0
    for i, t in enumerate(value_list):
        index += frequency[t]
        if index >= middle:
            splitVal = t
            split_index = i
            break
    else:
        print "Error: cannot find splitVal"
    try:
        nextVal = value_list[split_index + 1]
    except IndexError:
        nextVal = splitVal
    return (splitVal, nextVal, value_list[0], value_list[-1])


def split_numeric_value(numeric_value, splitVal):
    """
    split numeric value on splitVal
    return sub ranges
    """
    split_num = numeric_value.split(',')
    if len(split_num) <= 1:
        return split_num[0], split_num[0]
    else:
        low = split_num[0]
        high = split_num[1]
        # Fix 2,2 problem
        if low == splitVal:
            lvalue = low
        else:
            lvalue = low + ',' + splitVal
        if high == splitVal:
            rvalue = high
        else:
            rvalue = splitVal + ',' + high
        return lvalue, rvalue


def split_numeric(partition, dim, pwidth, pmiddle):
    sub_partitions = []
    # numeric attributes
    (splitVal, nextVal, low, high) = find_median(partition, dim)
    if low == '':
        pmiddle[dim] = '*'
        pwidth = (0, 0)
        partition.is_missing[dim] = True
    else:
        p_low = ATT_TREES[dim].dict[low]
        p_high = ATT_TREES[dim].dict[high]
        # update middle
        if low == high:
            pmiddle[dim] = low
        else:
            pmiddle[dim] = low + ',' + high
        pwidth[dim] = (p_low, p_high)
    if splitVal == '' or splitVal == nextVal:
        # update middle
        return []
    middle_pos = ATT_TREES[dim].dict[splitVal]
    lmiddle = pmiddle[:]
    rmiddle = pmiddle[:]
    lmiddle[dim], rmiddle[dim] = split_numeric_value(pmiddle[dim], splitVal)
    lhs = []
    rhs = []
    for temp in partition.member:
        pos = ATT_TREES[dim].dict[temp[dim]]
        if pos <= middle_pos:
            # lhs = [low, means]
            lhs.append(temp)
        else:
            # rhs = (mean, high]
            rhs.append(temp)
    lwidth = pwidth[:]
    rwidth = pwidth[:]
    lwidth[dim] = (pwidth[dim][0], middle_pos)
    rwidth[dim] = (ATT_TREES[dim].dict[nextVal], pwidth[dim][1])
    sub_partitions.append(Partition(lhs, lwidth, lmiddle))
    sub_partitions.append(Partition(rhs, rwidth, rmiddle))
    return sub_partitions


def split_categoric(partition, dim, pwidth, pmiddle):
    sub_partitions = []
    # categoric attributes
    splitVal = ATT_TREES[dim][partition.middle[dim]]
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
    flag = True
    nonempty_group = []
    for index, sub_group in enumerate(sub_groups):
        if len(sub_group) == 0:
            continue
        nonempty_group.append(index)
        if len(sub_group) < GL_K:
            flag = False
            break
    if flag:
        if len(nonempty_group) == 1:
            index = nonempty_group[0]
            # update middle
            pmiddle[dim] = sub_node[index].value
            pwidth[dim] = len(sub_node[index])
            return []
        for i, sub_group in enumerate(sub_groups):
            if len(sub_group) == 0:
                continue
            wtemp = pwidth[:]
            mtemp = pmiddle[:]
            wtemp[dim] = len(sub_node[i])
            mtemp[dim] = sub_node[i].value
            sub_partitions.append(Partition(sub_group, wtemp, mtemp))
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


def anonymize(partition):
    """
    Main procedure of Half_Partition.
    recursively partition groups until not allowable.
    """
    # print len(partition)
    # print partition.allow
    # pdb.set_trace()
    if check_splitable(partition) is False:
        RESULT.append(partition)
        return
    # Choose dim
    dim = choose_dimension(partition)
    if dim == -1:
        print "Error: dim=-1"
        pdb.set_trace()
    sub_partitions = split_partition(partition, dim)
    if len(sub_partitions) <= 1:
        partition.allow[dim] = 0
        anonymize(partition)
    else:
        for sub_p in sub_partitions:
            anonymize(sub_p)


def check_splitable(partition):
    """
    Check if the partition can be further splited while satisfying k-anonymity.
    """
    temp = sum(partition.allow)
    if temp == 0:
        return False
    return True


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


def mondrian(att_trees, data, K, QI_num=-1):
    """
    Mondrian for k-anonymity.
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
            # TODO Fix 6,6 bug
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
