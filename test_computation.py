import unittest

import computations


class TestInputSetting(unittest.TestCase):
    def test_set_input(self):
        g = computations.ComputationGraph()
        g.set_input('input.txt')
        self.assertTrue(1 == 1)

    def test_set_input_graph(self):
        g, gg = computations.ComputationGraph(), computations.ComputationGraph()
        g.set_input(gg)
        self.assertTrue(1 == 1)

    def test_set_input_raises_exception(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.set_input, ['input'])
        self.assertRaises(TypeError, g.set_input, 10)
        self.assertRaises(TypeError, g.set_input, object)


class TestMapAdd(unittest.TestCase):
    @staticmethod
    def correct_mapper(line):
        yield 1

    @staticmethod
    def incorrect_mapper(line, line1=None):
        yield 1

    @staticmethod
    def incorrect_mapper_2():
        yield 1

    def test_correct_mapper_add(self):
        g = computations.ComputationGraph()
        self.assertTrue(g.add_mapper(TestMapAdd.correct_mapper))

    def test_incorrect_mapper_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_mapper, TestMapAdd.incorrect_mapper)
        self.assertRaises(TypeError, g.add_mapper,
                          TestMapAdd.incorrect_mapper_2)

    def test_list_mapper_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_mapper, [TestMapAdd.correct_mapper])
        self.assertRaises(TypeError, g.add_mapper, (TestMapAdd.correct_mapper,))
        self.assertRaises(TypeError, g.add_mapper,
                          [TestMapAdd.correct_mapper,
                           TestMapAdd.incorrect_mapper])

    def test_number_mapper_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_mapper, 10)

    def test_string_mapper_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_mapper, 'mapper')

    def test_add_non_generator_function(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_mapper, lambda x: True)


class TestSortAdd(unittest.TestCase):
    def test_sort_one_key_add(self):
        g = computations.ComputationGraph()
        self.assertTrue(g.add_sort('key'))

    def test_sort_tuple_keys_add(self):
        g = computations.ComputationGraph()
        self.assertTrue(g.add_sort(('key',)))
        self.assertTrue(g.add_sort(('key1', 'key2')))

    def test_sort_list_keys_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_sort, ['key1', 'key2'])

    def test_sort_litter_keys_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_sort, 10)
        self.assertRaises(TypeError, g.add_sort, g)
        self.assertRaises(TypeError, g.add_sort, (10, 11))
        self.assertRaises(TypeError, g.add_sort, str)

    def test_sort_more_keys_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_sort, 'key1', 'key2')


class TestFolderAdd(unittest.TestCase):
    @staticmethod
    def folder(line, state):
        return 1

    @staticmethod
    def incorrect_folder(line):
        return 1

    @staticmethod
    def incorrect_folder_2(line, state):
        yield state

    begin_state = 0

    def test_correct_folder_add(self):
        g = computations.ComputationGraph()
        self.assertTrue(
            g.add_folder(TestFolderAdd.folder, TestFolderAdd.begin_state))

    def test_incorrect_folder_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_folder,
                          TestFolderAdd.incorrect_folder,
                          TestFolderAdd.begin_state)
        self.assertRaises(TypeError, g.add_folder,
                          TestFolderAdd.incorrect_folder_2,
                          TestFolderAdd.begin_state)

    def test_litter_folder_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_folder, 10, 11)
        self.assertRaises(TypeError, g.add_folder, [10], 10)


class TestReducerAdd(unittest.TestCase):
    @staticmethod
    def correct_reducer(columns):
        yield 1

    @staticmethod
    def incorrect_reducer():
        yield 2

    @staticmethod
    def incorrect_reducer_2(colums):
        return 1

    def test_correct_reducer_add(self):
        g = computations.ComputationGraph()
        self.assertTrue(g.add_reducer(TestReducerAdd.correct_reducer, 'key'))
        self.assertTrue(
            g.add_reducer(TestReducerAdd.correct_reducer, ('key1', 'key2')))

    def test_incorrect_reducer_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_reducer,
                          TestReducerAdd.correct_reducer, 10)
        self.assertRaises(TypeError, g.add_reducer,
                          TestReducerAdd.correct_reducer, ['key', 'key2'])
        self.assertRaises(TypeError, g.add_reducer,
                          TestReducerAdd.incorrect_reducer, 'key')
        self.assertRaises(TypeError, g.add_reducer,
                          TestReducerAdd.incorrect_reducer_2, 'key')

    def test_litter_reducer_add(self):
        g = computations.ComputationGraph()
        self.assertRaises(TypeError, g.add_reducer,
                          10, 11)
        self.assertRaises(TypeError, g.add_reducer,
                          'key', TestReducerAdd.correct_reducer)


class TestJoinAdd(unittest.TestCase):
    def test_correct_join_add(self):
        g1, g2 = computations.ComputationGraph(), computations.ComputationGraph()
        g3 = computations.ComputationGraph()
        self.assertTrue(g1.add_join((g2, 'input.txt'), ('key',), 'inner'))
        self.assertTrue(g1.add_join((g2, g3), ('key1', 'key2')))

    def test_incorrect_description_join_add(self):
        g1, g2 = computations.ComputationGraph(), \
                 computations.ComputationGraph()
        self.assertRaises(TypeError, g1.add_join, (g2,), ('key',))
        self.assertRaises(TypeError, g1.add_join, ('input.txt', g2), ('key',),
                          'inner')
        self.assertRaises(TypeError, g1.add_join, [g2, 'input.txt'], ('key',),
                          'inner')

    def test_incorrect_keys_join_add(self):
        g1 = computations.ComputationGraph()
        g2 = computations.ComputationGraph()
        g3 = computations.ComputationGraph()
        self.assertRaises(TypeError, g1.add_join, (g2, 'input.txt'), 'key')
        self.assertRaises(TypeError, g1.add_join, (g2, g3), ['key'])
        self.assertRaises(TypeError, g1.add_join, (g2, 'input.txt'), 10)

    def test_incorrect_strategy_join(self):
        g1 = computations.ComputationGraph()
        g2 = computations.ComputationGraph()
        self.assertRaises(TypeError, g1.add_join, (g2, 'input.txt'), ('key',),
                          10)
        self.assertRaises(TypeError, g1.add_join, (g2, 'input.txt'), ('key',),
                          'kek')

if __name__ == "__main__":
    unittest.main()