from operator import itemgetter
from itertools import groupby


class dictitemgetter:
    """Replica of itemgetter, always returning iterable from iterable"""
    def __init__(self, *args):
        self.keys = args

    def __call__(self, row):
        return [row[col] for col in self.keys]


def merge_dicts(left, right, ignored_keys=()):
    for key, val in right.items():
        if key not in ignored_keys and key in left:
            left["." + key] = val
        else:
            left[key] = val


class _Node:
    def __init__(self, source):
        """
        :param source: iterable or CompGraph
        """
        self.source = source

    def set_source(self, source):
        self.source = source


class _InitNode(_Node):
    def __init__(self, source, store_stream):
        super(_InitNode, self).__init__(source)
        self.store_stream = store_stream

    def __iter__(self):
        return self.run_init()

    def run_init(self):
        if not self.source:
            raise RuntimeError
        if self.store_stream:
            self.source = list(self.source)
        for row in self.source:
            yield row


class _MapNode(_Node):
    def __init__(self, source, mapper):
        super(_MapNode, self).__init__(source)
        self.mapper = mapper

    def __iter__(self):
        return self.run_map()

    def run_map(self):
        for row in self.source:
            for res_row in self.mapper(dict(row)):
                yield res_row


class _ReduceNode(_Node):
    def __init__(self, source, reducer, reduce_by):
        super(_ReduceNode, self).__init__(source)
        self.reducer = reducer
        self.reduce_by = reduce_by

    def __iter__(self):
        return self.run_reduce()

    def run_reduce(self):
        last_key = None
        igetter = dictitemgetter(*self.reduce_by)
        for key, rows in groupby(self.source, lambda row: dict((k, row[k]) for k in self.reduce_by)):
            if last_key is None or igetter(last_key) < igetter(key):
                for row in self.reducer(key, rows):
                    yield row
                last_key = key
            else:
                raise RuntimeError("Reduce: input table is not sorted")


class _SortNode(_Node):
    def __init__(self, source, sort_by):
        super(_SortNode, self).__init__(source)
        self.sort_by = sort_by

    def __iter__(self):
        return self.run_sort()

    def run_sort(self):
        for row in sorted(self.source, key=itemgetter(*self.sort_by)):
            yield row


class _FoldNode(_Node):
    def __init__(self, source, folder):
        super(_FoldNode, self).__init__(source)
        self.folder = folder

    def __iter__(self):
        return self.run_fold()

    def run_fold(self):
        yield self.folder(iter(self.source))


class _JoinNode(_Node):
    def __init__(self, source, strategy, on, join_by):
        super(_JoinNode, self).__init__(source)
        self.strategy = strategy
        self.on = on
        self.join_by = join_by

    def __iter__(self):
        if self.strategy == "inner":
            return self.run_inner_join()
        elif self.strategy == "left":
            return self.run_left_join()
        elif self.strategy == "right":
            return self.run_right_join()
        elif self.strategy == "outer":
            return self.run_outer_join()
        else:
            raise RuntimeError("Invalid join strategy")

    def next_group(self, generator, last_key, left=True):
        try:
            new_key, rows_for_key = next(generator)
            if last_key is not None and new_key < last_key:
                raise RuntimeError(f"Join: {'left' if left else 'right'} input table is not sorted")
        except StopIteration:
            return None, None
        return new_key, rows_for_key

    def join_keys(self, left_key, right_key, left_rows_for_key, right_rows_for_key, add_left_only, add_right_only):
        if left_key == right_key:
            for left_row in left_rows_for_key:
                right_rows_for_key = list(right_rows_for_key)
                for right_row in right_rows_for_key:
                    new_row = dict(left_row)
                    merge_dicts(new_row, right_row, self.join_by)
                    yield new_row
        elif left_key < right_key:
            if add_left_only:
                for left_row in left_rows_for_key:
                    yield left_row
        else:
            if add_right_only:
                for right_row in right_rows_for_key:
                    yield right_row

    def join_routine(self, left, right, add_left_only=False, add_right_only=False):
        if self.join_by:
            left_generator = groupby(left, dictitemgetter(*self.join_by))
            right_generator = groupby(right, dictitemgetter(*self.join_by))
        else:
            left_generator = groupby(left, lambda x: 0)  # returns the same for all rows
            right_generator = groupby(right, lambda x: 0)

        left_key, left_rows_for_key = self.next_group(left_generator, None)
        right_key, right_rows_for_key = self.next_group(right_generator, None, False)
        while left_key is not None and right_key is not None:
            for row in self.join_keys(
                    left_key, right_key, left_rows_for_key, right_rows_for_key, add_left_only, add_right_only
            ):
                yield row
            if left_key == right_key:
                left_key, left_rows_for_key = self.next_group(left_generator, left_key)
                right_key, right_rows_for_key = self.next_group(right_generator, right_key, False)
            elif left_key < right_key:
                left_key, left_rows_for_key = self.next_group(left_generator, left_key)
            else:
                right_key, right_rows_for_key = self.next_group(right_generator, right_key, False)

        if add_left_only and left_rows_for_key is not None:
            for row in self.iterate_remaining_in_join_routine(left_rows_for_key, left_generator, left_key):
                yield row
        if add_right_only and right_rows_for_key is not None:
            for row in self.iterate_remaining_in_join_routine(right_rows_for_key, right_generator, right_key, False):
                yield row

    def iterate_remaining_in_join_routine(self, rows_for_key, generator, key, left=True):
        while key is not None:
            for row in rows_for_key:
                yield row
            key, rows_for_key = self.next_group(generator, key, left)

    def run_inner_join(self):
        for row in self.join_routine(self.source, self.on._result):
            yield row

    def run_left_join(self):
        for row in self.join_routine(self.source, self.on._result, add_left_only=True):
            yield row

    def run_right_join(self):
        for row in self.join_routine(self.source, self.on._result, add_right_only=True):
            yield row

    def run_outer_join(self):
        for row in self.join_routine(self.source, self.on._result, add_left_only=True, add_right_only=True):
            yield row
