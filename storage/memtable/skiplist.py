import random
class Node:
    def __init__(self, key, value, level):
        self.key      = key
        self.value    = value
        self.forwards = [None] * (level + 1)  # pointer for each level [level 0, level 1, ..., level n]

class SkipList:
    def __init__(self, max_level=16, probability=0.5):
        self.max_level   = max_level    # max number of levels
        self.probability = probability  # coin flip probability (0.5 means 50% chance to go up a level )
        self.head        = Node(None, None, max_level)  # sentinel node
        self.level       = 0           # current highest level in use

    def __iter__(self):
        current = self.head.forwards[0]  # start at first real node
        while current:
            yield current.key, current.value
            current = current.forwards[0]

    def _random_level(self) -> int:
        level = 0
        # flip a coin until we get tails or reach the max level
        while random.random() < self.probability and level < self.max_level:
            level += 1
        return level

    def search(self, key):
        current = self.head
        # start from the highest level and move down
        for i in range(self.level, -1, -1):
            while current.forwards[i] and current.forwards[i].key < key:
                current = current.forwards[i] # move to the next node at level i
        current = current.forwards[0]  # move to the next node at level 0
        if current and current.key == key:
            return current.value  # found the key
        return None  # key not found

    def insert(self, key, value):
        update = [None] * (self.max_level + 1)  # track nodes that need to be updated
        current = self.head

        # find the position to insert the new node
        for i in range(self.level, -1, -1):
            while current.forwards[i] and current.forwards[i].key < key:
                current = current.forwards[i]
            update[i] = current  # store the last node at level i before the insertion point

        current = current.forwards[0]  # move to the next node at level 0

        if current and current.key == key:
            current.value = value  # key already exists, update the value
        else:
            new_level = self._random_level()  # determine the level for the new node
            if new_level > self.level:
                for i in range(self.level + 1, new_level + 1):
                    update[i] = self.head  # initialize new levels with head
                self.level = new_level  # update the current highest level

            new_node = Node(key, value, new_level)  # create a new node
            for i in range(new_level + 1):
                new_node.forwards[i] = update[i].forwards[i]  # link the new node to the next nodes
                update[i].forwards[i] = new_node  # link the previous nodes to the new node

    def delete(self, key):
        update = [None] * (self.max_level + 1)  # track nodes that need to be updated
        current = self.head

        # find the position of the node to delete
        for i in range(self.level, -1, -1):
            while current.forwards[i] and current.forwards[i].key < key:
                current = current.forwards[i]
            update[i] = current  # store the last node at level i before the deletion point

        current = current.forwards[0]  # move to the next node at level 0

        if current and current.key == key:
            for i in range(self.level + 1):
                if update[i].forwards[i] != current:
                    break
                update[i].forwards[i] = current.forwards[i]  # unlink the node from the list

            # remove levels that are no longer used
            while self.level > 0 and self.head.forwards[self.level] is None:
                self.level -= 1