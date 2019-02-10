#!/usr/bin/env python
"""
This module implements an interface to perform MapReduce computations with Python streams.
"""

from .node import _MapNode, _ReduceNode, _FoldNode, _SortNode, _JoinNode, _InitNode
from collections import defaultdict
from typing import Any, Iterable, Union, Dict, Callable, Sequence, Generator


class ComputeGraph:
    """
    Graph for MapReduce computations on streams/iterables.
    Supported operations - map, reduce, sort, join, fold.
    See algorithms.py for usage examples.
    """
    def __init__(self, source: Union[str, "ComputeGraph"]):
        """
        :param source: keyword arg to be used in run
                       or ComputeGraph instance
        """
        self._main_source = source
        self._main_source_name = source if isinstance(source, str) else None
        self._nodes = list()
        self._store = False
        self._result = None
        self._sources = dict()

    def add_map(self, mapper: Callable[[Dict[str, Any]], Generator[Dict[str, Any], None, None]]):
        """
        Add a map operation to the operations queue
        :param mapper: generator:
            takes one argument: next row of the table
            yields rows of a new table
            Example:
                def identity_mapper(row):
                    yield row
        """
        self._nodes.append(_MapNode(self._get_last_node(), mapper=mapper))

    def add_reduce(self, reducer: Callable[[Dict[str, Any], Dict[str, Any]], Generator[Dict[str, Any], None, None]],
                   reduce_by: Union[Iterable[str], str]):
        """
        Add a reduce operation to the operations queue
        :param reducer: generator:
            takes two arguments: dict with a key and iterator to rows with this key
            yields rows of a new table
            Example:
                def count_reducer(key, rows):
                    res = key
                    res["count"] = len(rows)
                    yield res
        :param reduce_by: column name or tuple of columns to be used as a key
        """
        if isinstance(reduce_by, str):
            reduce_by = (reduce_by,)
        self._nodes.append(_ReduceNode(self._get_last_node(), reducer=reducer, reduce_by=reduce_by))

    def add_sort(self, sort_by: Union[Iterable[str], str]):
        """
        Add a sort operation to the operations queue
        :param sort_by: column name or tuple of columns to be used as a key
        """
        if isinstance(sort_by, str):
            sort_by = (sort_by,)
        self._nodes.append(_SortNode(self._get_last_node(), sort_by=sort_by))

    def add_fold(self, folder: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]):
        """
        Add a fold operation to the operations queue
        :param folder: fuction:
            takes 1 argument: iterator to rows
            yields one row for a new table
            Example:
                def count_folder(rows):
                    return {"count": len(rows)}
        """
        self._nodes.append(_FoldNode(self._get_last_node(), folder=folder))

    def add_join(self, on: "ComputeGraph", join_by: Union[str, Iterable[str]] = (), strategy: str = "inner"):
        """
        Add a join operation to the operations queue
        :param on: ComputeGraph instance to join on
        :param join_by: column name or tuple of columns to be used to join
        :param strategy: type of join, options:
            "left" - left join,
            "right" - right join,
            "inner" - inner join,
            "outer" - full outer join
        """
        if isinstance(join_by, str):
            join_by = (join_by,)
        self._nodes.append(_JoinNode(self._get_last_node(), strategy, on=on, join_by=join_by))

    def run(self, **sources) -> Sequence[Dict[str, Any]]:
        """
        Run calculations for the graph and all its dependencies
        :param sources: iterables for inputs with names due to args, given to graphs' constructors
        :return: list of rows of the result table
        """
        sources = dict(sources)

        graphs = list()
        dfs_used_graphs = dict()

        for source_graph in sources.values():
            if not isinstance(source_graph, ComputeGraph):
                continue
            source_graph._topsort_dependent_graphs(graphs, dfs_used_graphs, **sources)

        self._topsort_dependent_graphs(graphs, dfs_used_graphs, **sources)

        source_usages = defaultdict(int)

        for g in graphs:
            g._result = None
            if g._main_source_name:
                source_usages[g._main_source_name] += 1

        source_nodes = dict()
        for g in graphs:
            g._set_source_node(sources, source_nodes, source_usages)
            if dfs_used_graphs[g] > 1:
                g._store = True

        for g in graphs:
            g._execute()

        for row in self._result:
            yield row

    def _set_source_node(self, sources, nodes, usages):
        if self._main_source_name:
            next_node = nodes.get(self._main_source_name)
            if not next_node:
                store_input_stream = (usages[self._main_source_name] > 1)
                next_node = _InitNode(
                    sources[self._main_source_name], store_stream=store_input_stream
                )
                nodes[self._main_source_name] = next_node
        else:
            next_node = _InitNode(self._main_source, store_stream=False)

        if not self._nodes:
            self._nodes.append(next_node)
        else:
            self._nodes[0].set_source(next_node)

    def _topsort_dependent_graphs(self, answer, used, **sources):
        used[self] = 1
        if self._nodes:
            for node in self._nodes:
                if isinstance(node, _JoinNode):
                    next_node = node.on
                elif isinstance(node.source, ComputeGraph):
                    next_node = node.source
                else:
                    next_node = None

                if next_node:
                    if next_node not in used:
                        next_node._topsort_dependent_graphs(answer, used, **sources)
                    else:
                        used[next_node] += 1

        if isinstance(self._main_source, ComputeGraph):
            if self._main_source not in used:
                self._main_source._topsort_dependent_graphs(answer, used, **sources)
            else:
                used[self._main_source] += 1

        answer.append(self)

    def _execute(self):
        self._store = True
        if self._store:
            self._result = list()
            for row in self._get_last_node():
                self._result.append(row)
        else:
            self._result = iter(self._get_last_node())

    def _get_last_node(self):
        return self._nodes[-1] if self._nodes else None

    def __iter__(self):
        return iter(self._result)
