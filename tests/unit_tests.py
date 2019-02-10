from compgraph import ComputeGraph
import pytest
from operator import itemgetter

COLUMN_KEY = "key"
COLUMN_VAL = "val"


def inc_val_mapper(row):
    row[COLUMN_VAL] += 1
    yield row


class TestLinearOperations:
    def test_empty_graph(self):
        input = [{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(10)]
        g = ComputeGraph(source="source")
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == input

    def test_map(self):
        input = [{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_KEY: i, COLUMN_VAL: i + 1} for i in range(10)]

        g = ComputeGraph(source="source")
        g.add_map(inc_val_mapper)
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_two_maps(self):
        input = [{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_KEY: i, COLUMN_VAL: i + 2} for i in range(10)]

        g = ComputeGraph(source="source")
        g.add_map(inc_val_mapper)
        g.add_map(inc_val_mapper)
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_reduce(self):
        def reducer(key, rows):
            res = dict(key)
            res[COLUMN_VAL] = sum(row[COLUMN_VAL] for row in rows)
            yield res

        input = [{COLUMN_KEY: i // 2, COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_KEY: i, COLUMN_VAL: 4 * i + 1} for i in range(5)]

        g = ComputeGraph(source="source")
        g.add_reduce(reducer, reduce_by=COLUMN_KEY)
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_fold(self):
        def folder(rows):
            res = dict()
            res[COLUMN_VAL] = sum([row[COLUMN_VAL] for row in rows])
            return res

        input = [{COLUMN_KEY: i // 2, COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_VAL: 45}]

        g = ComputeGraph(source="source")
        g.add_fold(folder)
        output = g.run(source=input)
        assert list(output) == etalon

    def test_sort(self):
        input = [{COLUMN_KEY: i} for i in range(10, 0, -1)]
        etalon = sorted(input, key=lambda row: row[COLUMN_KEY])

        g = ComputeGraph(source="source")
        g.add_sort(sort_by=COLUMN_KEY)
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon


class TestJoins:
    @pytest.fixture(
        scope="function", params=[
            ("inner", [{COLUMN_KEY: 1}]),
            ("left", [{COLUMN_KEY: 1}, {COLUMN_KEY: 2}]),
            ("right", [{COLUMN_KEY: 1}, {COLUMN_KEY: 3}]),
            ("outer", [{COLUMN_KEY: 1}, {COLUMN_KEY: 2}, {COLUMN_KEY: 3}]),
        ],
        ids=["inner", "left", "right", "outer"]
    )
    def join_params(self, request):
        return request.param

    def test_join(self, join_params):
        strategy, etalon = join_params

        left_source_for_join = [
            {COLUMN_KEY: 1},
            {COLUMN_KEY: 2},
        ]

        right_source_for_join = [
            {COLUMN_KEY: 1},
            {COLUMN_KEY: 3},
        ]

        g = ComputeGraph(source="left")
        h = ComputeGraph(source="right")
        g.add_join(h, join_by=COLUMN_KEY, strategy=strategy)
        output = g.run(left=left_source_for_join, right=right_source_for_join)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_join_same_colunms(self):
        g = ComputeGraph(source="source")
        h = ComputeGraph(source="source")

        g.add_join(h, join_by=COLUMN_KEY, strategy="inner")

        input = iter([{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(10)])
        etalon = [{COLUMN_KEY: i, COLUMN_VAL: i, "." + COLUMN_VAL: i} for i in range(10)]
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon


class TestInput:
    def test_init_from_graph(self):
        input = [{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_KEY: i, COLUMN_VAL: i + 1} for i in range(10)]

        g = ComputeGraph(source="g_source")
        h = ComputeGraph(source=g)
        h.add_map(inc_val_mapper)
        output = h.run(g_source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_run_from_graph(self):
        input = [{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_KEY: i, COLUMN_VAL: i + 1} for i in range(10)]

        g = ComputeGraph(source="g_source")
        h = ComputeGraph(source="h_source")
        h.add_map(inc_val_mapper)
        output = h.run(g_source=input, h_source=g)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_several_runs(self):
        g = ComputeGraph(source="source")
        g.add_map(inc_val_mapper)

        for run_number in range(5):
            input = [{COLUMN_KEY: i, COLUMN_VAL: i} for i in range(5 * (run_number + 1))]
            etalon = [{COLUMN_KEY: i, COLUMN_VAL: i + 1} for i in range(5 * (run_number + 1))]
            output = g.run(source=input)
            assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon

    def test_same_stream_input(self):
        g = ComputeGraph(source="source")
        h = ComputeGraph(source="source")

        g.add_join(h, join_by=COLUMN_KEY, strategy="inner")

        input = iter([{COLUMN_KEY: i} for i in range(10)])
        etalon = [{COLUMN_KEY: i} for i in range(10)]
        output = g.run(source=input)
        assert sorted(output, key=itemgetter(COLUMN_KEY)) == etalon


class TestStructure:
    def test_diamond_structure(self):
        a = ComputeGraph(source="a_source")
        b = ComputeGraph(source="b_source")
        c = ComputeGraph(source="b_source")
        c.add_map(inc_val_mapper)
        b.add_join(c, join_by=COLUMN_VAL, strategy="inner")

        input = [{COLUMN_VAL: i} for i in range(10)]
        etalon = [{COLUMN_VAL: i} for i in range(1, 10)]

        output = b.run(
            a_source=input,
            b_source=a,
        )
        assert sorted(output, key=itemgetter(COLUMN_VAL)) == etalon
