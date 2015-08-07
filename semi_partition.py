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
import time


__DEBUG = False
QI_LEN = 10
GL_K = 0
RESULT = []
ATT_TREES = []
QI_RANGE = []
IS_CAT = []


class Partition:

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


def getNormalizedWidth(partition, index):
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
        normWidth = getNormalizedWidth(partition, i)
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


def find_median(frequency):
    """
    find the middle of the partition
    return splitVal
    """
    splitVal = ''
    value_list = frequency.keys()
    value_list.sort(cmp=cmp_str)
    total = sum(frequency.values())
    middle = total / 2
    if middle < GL_K:
        print "Error: size of group less than 2*K"
        return ''
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
    return (splitVal, split_index)


def balance_partition(sub_partions, leftover):
    """
    balance partitions:
    Step 1: For partions with less than k records, merge them to leftover partition.
    Step 2: If leftover partition has less than k records, then move some records
    from partitions with more than k records.
    Step 3: After Step 2, if the leftover partition does not satisfy
    k-anonymity, then merge a partitions with k records to the leftover partition.
    Final: Backtrace leftover partition to the partent node.
    """
    if len(sub_partions) <= 1:
        # split failure
        return
    extra = 0
    check_list = []
    for sub_p in sub_partions[:]:
        temp = sub_p.member
        if len(temp) < GL_K:
            leftover.member.extend(temp)
            sub_partions.remove(sub_p)
        else:
            extra += len(temp) - GL_K
            check_list.append(sub_p)
    # there is no record to balance
    if len(leftover.member) == 0:
        return
    ls = len(leftover.member)
    if ls < GL_K:
        need_for_leftover = GL_K - ls
        if need_for_leftover > extra:
            min_p = 0
            min_size = len(check_list[0].member)
            for i, sub_p in enumerate(check_list):
                if len(sub_p.member) < min_size:
                    min_size = len(sub_p.member)
                    min_p = i
            sub_p = sub_partions.pop(min_p)
            leftover.member.extend(sub_p.member)
        else:
            while need_for_leftover > 0:
                check_list = [t for t in sub_partions if len(t.member) > GL_K]
                # TODO random pick
                for sub_p in check_list:
                    if need_for_leftover > 0:
                        t = sub_p.member.pop(random.randrange(len(sub_p.member)))
                        leftover.member.append(t)
                        need_for_leftover -= 1
    sub_partions.append(leftover)


def split_numeric_value(numeric_value, splitVal):
    """
    split numeric value on splitVal
    return sub ranges
    """
    stemp = numeric_value.split(',')
    if len(stemp) <= 1:
        return stemp[0], stemp[0]
    else:
        low = stemp[0]
        high = stemp[1]
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


def split_partition(partition, dim):
    """
    split partition and distribute records to different sub-partions
    """
    sub_partions = []
    pwidth = partition.width
    pmiddle = partition.middle
    if IS_CAT[dim] is False:
        # numeric attributes
        frequency = frequency_set(partition, dim)
        (splitVal, split_index) = find_median(frequency)
        if splitVal == '':
            print "Error: splitVal= null"
            pdb.set_trace()
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
                # rhs = (means, high]
                rhs.append(temp)
        lwidth = pwidth[:]
        rwidth = pwidth[:]
        # TODO need be changed to high and low
        lwidth[dim] = (pwidth[dim][0], split_index)
        rwidth[dim] = (split_index + 1, pwidth[dim][1])
        sub_partions.append(Partition(lhs, lwidth, lmiddle))
        sub_partions.append(Partition(rhs, rwidth, rmiddle))
    else:
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
                except:
                    continue
            else:
                print "Generalization hierarchy error!"
        for i, p in enumerate(sub_groups):
            if len(p) == 0:
                continue
            wtemp = pwidth[:]
            mtemp = pmiddle[:]
            wtemp[dim] = sub_node[i].support
            mtemp[dim] = sub_node[i].value
            sub_partions.append(Partition(p, wtemp, mtemp))
    return sub_partions


def anonymize(partition):
    """
    Main procedure of Half_Partition.
    recursively partition groups until not allowable.
    """
    global RESULT
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
    sub_partions = split_partition(partition, dim)
    balance_partition(sub_partions, leftover)
    if len(sub_partions) <= 1:
        partition.allow[dim] = 0
        sub_partions = [partition]
    # recursively partition
    for sub_p in sub_partions:
        anonymize(sub_p)


def check_splitable(partition):
    """
    Check if the partition can be further splited while satisfying k-anonymity.
    """
    if len(partition.member) < 2 * GL_K:
        return False
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


def semi_partition(att_trees, data, K, QI_num=-1):
    """
    Mondrian for l-diversity.
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
            QI_RANGE.append(ATT_TREES[i]['*'].support)
            wtemp.append(ATT_TREES[i]['*'].support)
            middle.append(ATT_TREES[i]['*'].value)
    whole_partition = Partition(data, wtemp, middle)
    start_time = time.time()
    anonymize(whole_partition)
    rtime = float(time.time() - start_time)
    ncp = 0.0
    for p in RESULT:
        r_ncp = 0.0
        for i in range(QI_LEN):
            r_ncp += getNormalizedWidth(p, i)
        temp = p.middle
        for i in range(len(p.member)):
            # TODO Fix 6,6 bug
            result.append(temp[:])
        r_ncp *= len(p.member)
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
        temp = [len(t.member) for t in RESULT]
        print sorted(temp)
        print "NCP = %.2f %%" % ncp
    return (result, (ncp, rtime))
