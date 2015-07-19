import unittest

from semi_partition import Partition
from utils.read_data import read_data, read_tree
from models.gentree import GenTree
import random

gl_K = 10
gl_rounds = 3
gl_QI_len = 5
gl_QI_range = [10, 10, 10, 10, 10]
# Build a GenTree object
gl_tree_temp = {}
tree = GenTree('*')
gl_tree_temp['*'] = tree
lt = GenTree('1,5', tree)
gl_tree_temp['1,5'] = lt
rt = GenTree('6,10', tree)
gl_tree_temp['6,10'] = rt
for i in range(1, 11):
    if i <= 5:
        t = GenTree(str(i), lt, True)
    else:
        t = GenTree(str(i), rt, True)
    gl_tree_temp[str(i)] = t


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


class functionTest(unittest.TestCase):
    def test_cmp_str(self):
        e1 = "12"
        e2 = "13"
        e3 = "0012"
        self.assertEqual(cmp_str(e1, e2), -1)
        self.assertEqual(cmp_str(e1, e3), 0)
        self.assertEqual(cmp_str(e2, e3), 1)

    def test_getNormalizedWidth(self):
        member = [['1', '1', '10', '10', '1'],
                  ['1', '3', '4', '2', '1'],
                  ['2', '2', '2', '2', '2'],
                  ['2', '2', '2', '2', '2'],
                  ['2', '2', '2', '2', '2'],
                  ['2', '2', '2', '2', '2'],
                  ['10', '10', '10', '10', '10']]
        partition = Partition(member, [5, 5, 10, 10, 10], ['*', '*', '*', '*', '*'])
        self.assertEqual(getNormalizedWidth(partition, 0), 0.5)
        self.assertEqual(getNormalizedWidth(partition, 2), 1)


if __name__ == '__main__':
    unittest.main()
