# !/usr/bin/env python
# coding=utf-8


import pdb
import random
from models.numrange import NumRange
from models.gentree import GenTree
import time


__DEBUG = False
gl_QI_len = 10
gl_K = 0
gl_result = []
gl_att_trees = []
gl_QI_ranges = []


class Partition:

    """Class for Group, which is used to keep records
    Store tree node in instances.
    self.member: records in group
    self.width: width of this partition on each domain
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
        self.allow = [1] * gl_QI_len


# def check_K_anonymity(partition):
#     """check if partition satisfy l-diversity
#     return True if satisfy, False if not.
#     """
#     sa_dict = {}
#     if isinstance(partition, Partition):
#         ltemp = partition.member
#     else:
#         ltemp = partition
#     if len(ltemp) >= gl_K:
#         return True
#     return False


def cmp_str(element1, element2):
    """compare number in str format correctley
    """
    return cmp(int(element1), int(element2))


def getNormalizedWidth(partition, index):
    """return Normalized width of partition
    similar to NCP
    """
    width = partition.width[index]
    return width * 1.0 / gl_QI_ranges[index]


def choose_dimension(partition):
    """chooss dim with largest normlized Width
    return dim index.
    """
    max_width = -1
    max_dim = -1
    for i in range(gl_QI_len):
        if partition.allow[i] == 0:
            continue
        normWidth = getNormalizedWidth(partition, i)
        if normWidth > max_width:
            max_width = normWidth
            max_dim = i
    if max_width > 1:
        print "Error: max_width > 1"
        pdb.set_trace()
    return max_dim


def frequency_set(partition, dim):
    """get the frequency_set of partition on dim
    return dict{key: str values, values: count}
    """
    frequency = {}
    for record in partition.member:
        try:
            frequency[record[dim]] += 1
        except:
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
    if middle < gl_K:
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
        if len(temp) < gl_K:
            leftover.member.extend(temp)
            sub_partions.remove(sub_p)
        else:
            extra += len(temp) - gl_K
            check_list.append(sub_p)
    # there is no record to balance
    if len(leftover.member) == 0:
        return
    ls = len(leftover.member)
    if ls < gl_K:
        need_for_leftover = gl_K - ls
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
                check_list = [t for t in sub_partions if len(t.member) > gl_K]
                # TODO random pick
                for sub_p in check_list:
                    if need_for_leftover > 0:
                        t = sub_p.member.pop(random.randrange(len(sub_p.member)))
                        leftover.member.append(t)
                        need_for_leftover -= 1
    sub_partions.append(leftover)


def split_partition(partition, dim):
    """
    split partition and distribute records to different sub-partions
    """
    sub_partions = []
    pwidth = partition.width
    pmiddle = partition.middle
    if isinstance(gl_att_trees[dim], NumRange):
        # numeric attributes
        frequency = frequency_set(partition, dim)
        (splitVal, split_index) = find_median(frequency)
        if splitVal == '':
            print "Error: splitVal= null"
            pdb.set_trace()
        middle_pos = gl_att_trees[dim].dict[splitVal]
        lmiddle = pmiddle[:]
        rmiddle = pmiddle[:]
        temp = pmiddle[dim].split(',')
        low = temp[0]
        high = temp[1]
        lmiddle[dim] = low + ',' + splitVal
        rmiddle[dim] = splitVal + ',' + high
        lhs = []
        rhs = []
        for temp in partition.member:
            pos = gl_att_trees[dim].dict[temp[dim]]
            if pos <= middle_pos:
                # lhs = [low, means]
                lhs.append(temp)
            else:
                # rhs = (means, high]
                rhs.append(temp)
        lwidth = pwidth[:]
        rwidth = pwidth[:]
        # TODO need be changed to high and low
        lwidth[dim] = split_index + 1
        rwidth[dim] = pwidth[dim] - split_index - 1
        sub_partions.append(Partition(lhs, lwidth, lmiddle))
        sub_partions.append(Partition(rhs, rwidth, rmiddle))
    else:
        # categoric attributes
        if partition.middle[dim] != '*':
            splitVal = gl_att_trees[dim][partition.middle[dim]]
        else:
            splitVal = gl_att_trees[dim]['*']
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
    global gl_result
    if check_splitable(partition) is False:
        gl_result.append(partition)
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
    if len(partition.member) < 2 * gl_K:
        return False
    temp = sum(partition.allow)
    if temp == 0:
        return False
    return True


def semi_partition(att_trees, data, K):
    """Mondrian for l-diversity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    global gl_K, gl_result, gl_QI_len, gl_att_trees, gl_QI_ranges
    gl_att_trees = att_trees
    middle = []
    gl_QI_len = len(data[0]) - 1
    gl_K = K
    gl_result = []
    result = []
    gl_QI_ranges = []
    for i in range(gl_QI_len):
        if isinstance(gl_att_trees[i], NumRange):
            gl_QI_ranges.append(gl_att_trees[i].range)
            middle.append(gl_att_trees[i].value)
        else:
            gl_QI_ranges.append(gl_att_trees[i]['*'].support)
            middle.append(gl_att_trees[i]['*'].value)
    partition = Partition(data, gl_QI_ranges[:], middle)
    start_time = time.time()
    anonymize(partition)
    rtime = float(time.time() - start_time)
    ncp = 0.0
    for p in gl_result:
        r_ncp = 0.0
        for i in range(gl_QI_len):
            r_ncp += getNormalizedWidth(p, i)
        temp = p.middle
        for i in range(len(p.member)):
            result.append(temp[:])
        r_ncp *= len(p.member)
        ncp += r_ncp
    ncp /= gl_QI_len
    ncp /= len(data)
    ncp *= 100
    if __DEBUG:
        print "K=%d" % K
        print "size of partitions"
        print len(gl_result)
        temp = [len(t.member) for t in gl_result]
        print sorted(temp)
        print "NCP = %.2f %%" % ncp
    return (result, (ncp, rtime))
