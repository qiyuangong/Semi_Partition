# !/usr/bin/env python
# coding=utf-8


import pdb
from models.numrange import NumRange
from models.gentree import GenTree


__DEBUG = True
gl_QI_len = 10
gl_K = 0
gl_result = []
gl_att_trees = []
gl_QI_range = []


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


def list_to_str(value_list, cmpfun=cmp, sep=';'):
    """covert sorted str list (sorted by cmpfun) to str
    value (splited by sep). This fuction is value safe, which means
    value_list will not be changed.
    return str list.
    """
    temp = value_list[:]
    temp.sort(cmp=cmpfun)
    return sep.join(temp)


def check_K_anonymity(partition):
    """check if partition satisfy l-diversity
    return True if satisfy, False if not.
    """
    sa_dict = {}
    if isinstance(partition, Partition):
        ltemp = partition.member
    else:
        ltemp = partition
    if len(ltemp) >= gl_K:
        return True
    return False


def cmp_str(element1, element2):
    """compare number in str format correctley
    """
    return cmp(int(element1), int(element2))


def getNormalizedWidth(partition, index):
    """return Normalized width of partition
    similar to NCP
    """
    width = partition.width[index]
    return width * 1.0 / gl_QI_range[index]


def choose_dimension(partition):
    """chooss dim with largest normlized Width
    return dim index.
    """
    max_witdh = -1
    max_dim = -1
    for i in range(gl_QI_len):
        if partition.allow[i] == 0:
            continue
        normWidth = getNormalizedWidth(partition, i)
        if normWidth > max_witdh:
            max_witdh = normWidth
            max_dim = i
    if max_witdh > 1:
        print "Error: max_witdh > 1"
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
    for sub_p in sub_partions[:]:
        temp = sub_p.member
        if len(temp) < gl_K:
            leftover.member.extend(temp)
            sub_partions.remove(temp)
        else:
            
    ls = len(leftover.member)
    if ls >= gl_K:
        sub_partions.append(leftover)
    else:
        remain = gl_K - ls




def anonymize(partition):
    """
    Main procedure of Half_Partition.
    recursively partition groups until not allowable.
    """
    global gl_result
    if check_splitable(partition) is False:
        gl_result.append(partition)
    sub_partions = []
    pwidth = partition.width
    pmiddle = partition.middle
    leftover = Partition([],pwidth, pmiddle)
    # Choose dim
    dim = choose_dimension(partition)
    if dim == -1:
        print "Error: dim=-1"
        pdb.set_trace()
    if isinstance(gl_att_trees[dim], NumRange):
        # numeric attributes
        pass
    else:
        # cat attributes
        pass
    # balance sub-partitions
    balance_partition(sub_partions, leftover)
    # recursively partition
    for sub_p in sub_partions:
        anonymize(sub_p)


def check_splitable(partition):
    """
    Check if the partition can be further splited while satisfying k-anonymity.
    """
    if len(len(partition) < 2* gl_K):
        return False
    temp = sum(partition.allow)
    if temp > 0:
        return False
    return True


def half_partition(att_trees, data, K):
    """Mondrian for l-diversity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    print "K=%d" % K
    global gl_K, gl_result, gl_QI_len, gl_att_trees, gl_QI_range
    gl_att_trees = att_trees
    middle = []
    gl_QI_len = len(data[0]) - 1
    gl_K = K
    gl_result = []
    result = []
    gl_QI_range = []
    for i in range(gl_QI_len):
        if isinstance(gl_att_trees[i], NumRange):
            gl_QI_range.append(gl_att_trees[i].range)
            middle.append(gl_att_trees[i].value)
        else:
            gl_QI_range.append(gl_att_trees[i]['*'].support)
            middle.append(gl_att_trees[i]['*'].value)
    partition = Partition(data, gl_QI_range[:], middle)
    anonymize(partition)
    ncp = 0.0
    for p in gl_result:
        rncp = 0.0
        for i in range(gl_QI_len):
            rncp += getNormalizedWidth(p, i)
        temp = p.middle
        for i in range(len(p.member)):
            result.append(temp[:])
        rncp *= len(p.member)
        ncp += rncp
    ncp /= gl_QI_len
    ncp /= len(data)
    ncp *= 100
    if __DEBUG:
        print "size of partitions"
        print len(gl_result)
        # print [len(t.member) for t in gl_result]
        print "NCP = %.2f %%" % ncp
        # pdb.set_trace()
    return result
