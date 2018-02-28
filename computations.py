import functools
import json
from inspect import isgeneratorfunction, signature
from operator import itemgetter


def operation_deprecated(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print('{} will be supported in next version (probably)!'.format(
            func.__name__))
        return func(*args, **kwargs)

    return wrapper


class Operation(object):
    """ Abstract class for operations
    """

    def __init__(self, _input=None, _output=None):
        self.input = _input
        self.output = _output
        self.table = []

    def __call__(self, table):
        self.table = table
        yield None

    @operation_deprecated
    def set_input(self, _input):
        """
        :param _input: string -- json file_path
                       ComputationGraph -- takes result as an input
                       Operation -- takes result as an input
                       list -- takes list of dict-lice objects as input
        :return:
        """
        if not (isinstance(_input, str)
                or isinstance(_input, list)
                or isinstance(_input, ComputationGraph)
                or isinstance(_input, Operation)):
            raise TypeError("Incorrect input type")
        else:
            self.input = _input

    @operation_deprecated
    def read_input(self):
        """
        Reads input from self.input. Input must be set before use
        :return:
        """
        if isinstance(self.input, str):
            with open(self.input, 'r') as input:
                for line in input.readlines():
                    self.table.append(json.loads(line))

        if isinstance(self.input, ComputationGraph):
            if self.input.is_counted:
                self.table = self.input.result
            else:
                raise RuntimeError('All graphs must be counted '
                                   'before use as an input')

        if isinstance(self.input, Operation):
            self.table = list(self.input.__call__())
            if self.table is None:
                raise AttributeError("Operation in nodes must be stated")

        if isinstance(self.input, list):
            self.table = self.input


class Map(Operation):
    def __init__(self, mapper, _input=None, _output=None):
        super().__init__(_input, _output)
        self.mapper = None
        if isgeneratorfunction(mapper):
            sig = signature(mapper)
            params = sig.parameters
            if len(params) == 1:
                self.mapper = mapper
            else:
                raise TypeError("Mapper must take one argument")
        else:
            raise TypeError("Mapper must be a generator function")

    def __call__(self, table):
        """
        :param table: a table to apply map-function on
        :return: for each line in table, yields results from mapper
        """

        self.table = table
        for line in self.table:
            yield from self.mapper(line)


class Sort(Operation):
    def __init__(self, keys, _input=None, _output=None):
        super().__init__(_input, _output)
        if isinstance(keys, str):
            self.keys = (keys,)
        elif isinstance(keys, tuple):
            self.keys = keys
            for key in keys:
                if not isinstance(key, str):
                    raise TypeError('Tuple must contain only strings')
        else:
            raise TypeError('Keys to sort must be string '
                            'or tuple of string')

    def __call__(self, table):
        """
        Sorts table in increasing order, using values in self.keys to
        compare lines
        :param table: a table to sort
        :return: yields lines from sorted table
        """

        self.table = table
        self.table = sorted(self.table, key=itemgetter(*self.keys))
        for line in self.table:
            yield line


class Fold(Operation):
    def __init__(self, folder, begin_state, _input=None, _output=None):
        super().__init__(_input, _output)
        self.folder = None
        if callable(folder) and not isgeneratorfunction(folder):
            sig = signature(folder)
            params = sig.parameters
            if len(params) == 2:
                self.folder = folder
            else:
                raise TypeError("Folder function must get 2 arguments")
        else:
            raise TypeError("Folder must be a callable non-generator"
                            "function")
        self.state = begin_state

    def __call__(self, table):
        """
        Applies fold function to a table, updates state with result
        :param table: A table to apply function
        :return: yields new state
        """

        self.table = table
        for line in self.table:
            self.state = self.folder(line, self.state)
        yield self.state


class Reduce(Operation):
    def __init__(self, reducer, columns, _input=None, _output=None):
        super().__init__(_input, _output)
        self.reducer = None
        if isgeneratorfunction(reducer):
            sig = signature(reducer)
            params = sig.parameters
            if len(params) == 1:
                self.reducer = reducer
            else:
                raise TypeError("Reducer requires one argument")
        else:
            raise TypeError("Reducer must be a generator function")

        if isinstance(columns, str):
            self.columns = (columns,)
        elif isinstance(columns, tuple):
            for column in columns:
                if not isinstance(column, str):
                    raise TypeError('Columns must be strings')
            self.columns = columns
        else:
            raise TypeError('Reducers columns must be '
                            'string or tuple of string')

    def __call__(self, table):
        """
        Applies reduce function to table in following way:
        sorts table,
        creates blocks with equal values in stated columns
        yields folder result on each block
        :param table: A table to apply function
        :return: yields results from reducer
        """

        self.table = table
        self.table = sorted(self.table, key=itemgetter(*self.columns))

        def check_equal(first_line, second_line):
            equal = True
            for column in self.columns:
                equal &= (first_line[column] == second_line[column])
            return equal

        bucket = []
        for line in self.table:
            if len(bucket) == 0:
                bucket.append(line)
                continue
            if check_equal(bucket[-1], line):
                bucket.append(line)
            else:
                yield from self.reducer(bucket)
                bucket = [line]


class Join(Operation):
    def __init__(self, on, keys, strategy,
                 _input=None, _output=None):
        super().__init__(_input, _output)
        self.on = on
        if not isinstance(keys, tuple):
            raise TypeError('Join columns must be tuple')
        self.keys = keys
        if strategy not in ['outer', 'left', 'right', 'inner', 'cross']:
            raise TypeError('Strategy is not supported')
        self.strategy = strategy
        if len(self.keys) == 0:
            if self.strategy == 'outer':
                self.strategy = 'cross'

    def __call__(self, table):
        """
        Joins table with table from stated graph,
        graph must be counted before use
        :param table: A table to join graph result with
        :return: resulting table
        """

        self.table = table
        if not self.on.is_counted:
            raise RuntimeError("Graph must be computed before use in join")

        self.to_join = self.on.result

        if len(self.keys) > 0:
            self.table = sorted(self.table, key=itemgetter(*self.keys))
            self.to_join = sorted(self.to_join, key=itemgetter(*self.keys))

        common = (set(self.table[0].keys()) & set(self.to_join[0].keys())) \
                 - set(self.keys)

        for column in common:
            for left in self.table:
                left["left_" + column] = left.pop(column)

            for right in self.to_join:
                right["right_" + column] = right.pop(column)

        self.left_keys = list(self.table[0].keys())
        self.right_keys = list(self.to_join[0].keys())

        if self.strategy == "inner":
            return list(self.__inner_join())
        elif self.strategy == "left":
            return list(self.__left_join())
        elif self.strategy == "right":
            return list(self.__right_join())
        elif self.strategy == "outer":
            return list(self.__outer_join())
        elif self.strategy == "cross":
            return list(self.__cross_join())

    def __apply_reducer(self, table, reducer, keys):
        if len(keys) > 1:
            table = sorted(table, key=itemgetter(*keys))

        def check_equil(first_line, second_line):
            equal = True
            for column in keys:
                equal &= (first_line[column] == second_line[column])
            return equal

        bucket = []
        for line in table:
            if len(bucket) == 0:
                bucket.append(line)
                continue
            if check_equil(bucket[-1], line):
                bucket.append(line)
            else:
                yield from reducer(bucket)
                bucket = [line]

    def __inner_reducer(self, records):
        """
        Reducer to use in inner join strategy
        :param records:
        :return:
        """

        if len(records) > 1:
            first_elem_keys = records[0].keys()
            second_elem_keys = records[1].keys()
            neq_found = False

            for first_key, second_key in zip(first_elem_keys,
                                             second_elem_keys):
                if first_key != second_key:
                    neq_found = True
                    break

            if neq_found:
                for value in records[1:]:
                    value.update(records[0])
                    new_line = {}
                    for key in sorted(value):
                        new_line[key] = value[key]
                    yield new_line
            else:
                for value in records[:-1]:
                    value.update(records[-1])
                    new_line = {}
                    for key in sorted(value):
                        new_line[key] = value[key]
                    yield new_line

    def __left_reducer(self, records):
        """
        Reducer to use in left join strategy
        :param records:
        :return:
        """

        if len(records) > 1:
            yield from self.__inner_reducer(records)

        if len(records) == 1:
            for k1, k2 in zip(records[0].keys(), self.left_keys):
                if k1 != k2:
                    break
            else:
                records[0].update({key: None for key in self.right_keys
                                   if key not in self.left_keys})

                new_line = {}
                for key in sorted(records[0]):
                    new_line[key] = records[0][key]
                yield new_line

    def __right_reducer(self, records):
        """
        Reducer to use in right join strategy
        :param records:
        :return:
        """

        if len(records) > 1:
            yield from self.__inner_reducer(records)

        if len(records) == 1:
            for k1, k2 in zip(records[0].keys(), self.right_keys):
                if k1 != k2:
                    break
            else:
                records[0].update({key: None for key in self.left_keys
                                   if key not in self.right_keys})

                new_line = {}
                for key in sorted(records[0]):
                    new_line[key] = records[0][key]
                yield new_line

    def __outer_reducer(self, records):
        """
        Reducer to use in outer join strategy
        :param records:
        :return:
        """

        if len(records) > 1:
            yield from self.__inner_reducer(records)

        if len(records) == 1:
            summary = {key: value for key, value in records[0].items()}

            for k1, k2 in zip(records[0].keys(), self.right_keys):

                if k1 != k2:
                    summary.update({key: None for key in self.right_keys
                                    if key not in self.left_keys})
                    break

            else:
                summary.update({key: None for key in self.left_keys
                                if key not in self.right_keys})

            new_line = {}
            for key in sorted(summary):
                new_line[key] = summary[key]
            yield new_line

    def __inner_join(self):
        both = sorted(self.table + self.to_join, key=itemgetter(*self.keys))
        yield from self.__apply_reducer(both, self.__inner_reducer,
                                        self.keys)

    def __left_join(self):
        both = sorted(self.table + self.to_join, key=itemgetter(*self.keys))
        yield from self.__apply_reducer(both, self.__left_reducer,
                                        self.keys)

    def __right_join(self):
        both = sorted(self.table + self.to_join, key=itemgetter(*self.keys))
        yield from self.__apply_reducer(both, self.__right_reducer,
                                        self.keys)

    def __outer_join(self):
        both = sorted(self.table + self.to_join, key=itemgetter(*self.keys))
        yield from self.__apply_reducer(both, self.__outer_reducer, self.keys)

    def __cross_join(self):
        for first_dict in self.table:
            for second_dict in self.to_join:
                first_dict.update(second_dict)
                new_line = {}
                for key in sorted(first_dict):
                    new_line[key] = first_dict[key]
                yield new_line


class ComputationGraph(object):
    """
    Simple ComputationGraph implementation

    Functions: add_mapper, add_sort, add_folder,
               add_reducer, add_join, set_input.
    """

    def __init__(self):
        self.dependencies = []
        self.dependencies_input = []
        self.table = []
        self.result = []
        self.is_counted = False
        self.counted_input = None
        self.operations = []
        self.__input = None
        self.__output = None

    def set_input(self, filename):
        """
        States input file for current graph instance, does nothing if graph
        was counted on that input and result was saved.
        :param filename: path to file OR other ComputationGraph -- its result
        will be use as an input to this instance
        :return:
        """

        if filename == self.counted_input and self.is_counted:
            return
        if not (isinstance(filename, str) or isinstance(filename,
                                                        ComputationGraph)):
            raise TypeError("Wrong input type for ComputationGraph")
        self.__input = filename
        self.is_counted = False

    def __read_input(self):
        if isinstance(self.__input, str):
            with open(self.__input, 'r') as input:
                for line in input.readlines():
                    self.table.append(json.loads(line))
        elif isinstance(self.__input, ComputationGraph):
            self.__input.run()
            self.table = self.__input.result.copy()

    def __count_dependencies(self):
        for g, input in zip(self.dependencies, self.dependencies_input):
            g.set_input(input)

        for g in self.dependencies:
            g.run()

    def add_mapper(self, mapper):
        """
        Adds mapper-node to graph
        :param mapper: mapper function. Must be generator
        :return: True on success
        """

        self.operations.append(Map(mapper))
        return True

    def add_sort(self, keys):
        """
        Adds sort-node to graph
        :param keys: keys to get values from in sort
         Must be str -- the name of column to sort values
              or tuple of str -- name of columns
        :return: True on success
        """

        self.operations.append(Sort(keys))
        return True

    def add_folder(self, folder, begin_state):
        """
        Adds folder-node to graph
        :param folder: folder function, Must be callable
        :param begin_state:  begin_state to use in folder
        :return: True on success
        """

        self.operations.append(Fold(folder, begin_state))
        return True

    def add_reducer(self, reducer, keys):
        """
        Adds reducer-node to graph
        :param reducer: reducer function, must be generator
        :param keys: keys to create buckets for reducer function
        :return: True on success
        """

        self.operations.append(Reduce(reducer, keys))
        return True

    def add_join(self, gr_description, keys, strategy=None):
        """
        Adds join-node to graph
        :param gr_description: tuple of length 2:
        description of a graph instance, which result will be used in join
        gr_description[0] -- graph instance
        gr_description[1] -- input for graph.
        Might be path to file or other graph
        :param keys: keys to use in join strategy
        :param strategy: inner, left, right, outer, cross -- SQL type operation
        :return: True on success
        """
        if not isinstance(gr_description, tuple):
            raise TypeError('No graph description provided')
        if len(gr_description) != 2:
            raise TypeError('Wrong graph description format')
        on, on_input = gr_description
        if not isinstance(on, ComputationGraph):
            raise TypeError('Can join only graph to graph')
        if strategy is None:
            strategy = 'cross'

        self.dependencies.append(on)
        self.dependencies_input.append(on_input)

        self.operations.append(Join(on, keys, strategy))
        return True

    def run(self):
        """
        Computes graph result. Input must be stated before use
        :return:
        """

        if self.is_counted and self.__input == self.counted_input:
            return
        else:
            self.counted_input = None
            self.is_counted = False

        self.__count_dependencies()

        self.__read_input()
        _table = self.table.copy()

        for operation in self.operations:
            _table = list(operation(_table))
        self.result = _table
        self.is_counted = True
        self.counted_input = self.__input

    def write_output(self, filename):
        """
        Writes result to filename
        :param filename: path to file
        :return:
        """

        if not self.is_counted:
            raise RuntimeError('Graph must be counted before writing result')

        with open(filename, 'w') as output:
            for line in self.result:
                output.write(json.dumps(line))
