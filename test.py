from btree import *
import pdb

### build tree
btree = core.BTree(4)
root = core.Node([13, 24, 30], 4)
child_one = core.Node([2, 3, 5, 7], 4, 14)
child_two = core.Node([14, 16, 19, 22], 4, 24)
child_three = core.Node([24, 27, 29], 4, 33)
child_four = core.Node([33, 34, 38, 39], 4)


root.acquire_write()
root.set_children_ids([child_one.id, child_two.id, child_three.id, child_four.id])
root.release_write()
child_one.link = child_two.id
child_two.link = child_three.id
child_three.link = child_four.id

btree.root_id = root.id
btree.print()
### Test adding keys

# Add key to leaf
btree.add_key(28)
# Add key causing leaf split, while parent(root) has space
btree.add_key(20)
btree.print()

# Add key causing leaf split, and parent(root) split -> increase depth of tree
btree.add_key(8)

# Add some more keys
btree.add_key(10)
btree.add_key(37)

btree.print()

### Test removing keys
# Remove key from leaf
btree.remove_key(39)
btree.remove_key(28)

# Remove key from leaf, causing rotation right (from left sibling)
btree.remove_key(33)

# Remove key from leaf, causing rotation left (from right sibling)
btree.remove_key(3)

# Remove key from leaf, causing merge left (from right sibling)
btree.remove_key(37)

# Setup for next test
btree.add_key(37)
btree.add_key(39)
btree.remove_key(24)

# # Remove key from leaf, causing merge right (from left sibling)
btree.remove_key(27)

# # Remove additional key to setup next test
btree.remove_key(16)

# # Remove key causing rebalance propagating to the root, emptying it, and
# # creating a new root
btree.remove_key(8)

btree.print()
print("done")
