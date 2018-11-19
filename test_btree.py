import unittest
import btree
import numpy
import pdb

class TestNode(unittest.TestCase):

    def setUp(self):
        self.node = btree.Node([])

    def test_count(self):
        self.assertEqual(self.node.count(), 0, 'Incorrect starting count')

    def test_add_key(self):
        self.node.add_key(1)
        self.assertEqual(self.node.keys, [1], 'Incorrectly adds first key')
        self.node.add_key(2)
        self.assertEqual(self.node.keys, [1,2], 'Incorrectly adds a second key')

    def test_split(self):
        self.node.add_key(1)
        self.node.add_key(2)

        res = self.node.add_key(3)
        expected_res = {'median': 2, 'left': btree.Node([1]),'right': btree.Node([3])}

        self.assertEqual(2, expected_res['median'], 'Does not properly find median')
        self.assertEqual([1], expected_res['left'].keys, 'Does not properly create left node')
        self.assertEqual([3], expected_res['right'].keys, 'Does not properly create right node')

class TestBTree(unittest.TestCase):
    def setUp(self):
        self.btree = btree.BTree()

    def test_first_split(self):
        self.btree.add_key(1)
        self.btree.add_key(2)
        self.btree.add_key(3)

        root = self.btree.root
        left_child = root.children[0]
        right_child = root.children[1]
        self.assertEqual(root.keys, [2], 'Does not choose the correct root on split')
        self.assertEqual(left_child.keys, [1], 'Does not create the correct left child on split')
        self.assertEqual(right_child.keys, [3], 'Does not create the correct right child on split')

    def test_second_split(self):
        self.btree.add_key(1)
        self.btree.add_key(2)
        self.btree.add_key(3)
        self.btree.add_key(4)
        self.btree.add_key(5)

        root = self.btree.root
        left_child = root.children[0]
        middle_child = root.children[1]
        right_child = root.children[2]
        self.assertEqual(root.keys, [2, 4], 'Does not create the correct root on second split')
        self.assertEqual(left_child.keys, [1], 'Does not create the correct left child on split')
        self.assertEqual(middle_child.keys, [3], 'Does not create the correct middle child on split')
        self.assertEqual(right_child.keys, [5], 'Does not create the correct right child on split')

    def test_third_split(self):
        self.btree.add_key(1)
        self.btree.add_key(2)
        self.btree.add_key(3)
        self.btree.add_key(4)
        self.btree.add_key(5)
        self.btree.add_key(6)
        self.btree.add_key(7)

        root = self.btree.root
        left_child = root.children[0]
        right_child = root.children[1]
        left_left_child = left_child.children[0]
        left_right_child = left_child.children[1]
        right_left_child = right_child.children[0]
        right_right_child = right_child.children[1]

        self.assertEqual(root.keys, [4], 'Does not create the correct root on second split')
        self.assertEqual(left_child.keys, [2], 'Does not create the correct left child on split')
        self.assertEqual(right_child.keys, [6], 'Does not create the correct right child on split')
        self.assertEqual(left_left_child.keys, [1], 'Does not create the correct right child on split')
        self.assertEqual(left_right_child.keys, [3], 'Does not create the correct right child on split')
        self.assertEqual(right_left_child.keys, [5], 'Does not create the correct right child on split')
        self.assertEqual(right_right_child.keys, [7], 'Does not create the correct right child on split')


    # Currently unsure how to test this - it fails sometimes, as the BTree
    # ends up being structured differently depending on the insertion
    # order, though these different structures all still represent valid
    # BTrees - it just depends on which leaf is forced to split first.
    def test_randomization_of_insertion_order(self):
        keys = [1, 2, 3, 4, 5, 6, 7]

        while len(keys) > 0:
            samp = numpy.random.choice(keys)
            self.btree.add_key(samp)
            # if self.btree.num_keys >= 5:
            #     pdb.set_trace()
            keys.remove(samp)

        root = self.btree.root
        left_child = root.children[0]
        right_child = root.children[1]
        left_left_child = left_child.children[0]
        left_right_child = left_child.children[1]
        right_left_child = right_child.children[0]
        right_right_child = right_child.children[1]

        self.assertEqual(root.keys, [4], 'Does not create the correct root on second split')
        self.assertEqual(left_child.keys, [2], 'Does not create the correct left child on split')
        self.assertEqual(right_child.keys, [6], 'Does not create the correct right child on split')
        self.assertEqual(left_left_child.keys, [1], 'Does not create the correct right child on split')
        self.assertEqual(left_right_child.keys, [3], 'Does not create the correct right child on split')
        self.assertEqual(right_left_child.keys, [5], 'Does not create the correct right child on split')
        self.assertEqual(right_right_child.keys, [7], 'Does not create the correct right child on split')

if __name__ == '__main__':
    unittest.main()
