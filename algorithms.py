from compgraph import ComputeGraph

from math import log
from collections import defaultdict
import heapq
import datetime
from math import cos, sin, radians, atan2, sqrt


def split_word_map(row):
    for w in row["text"].split():
        word = "".join(ch for ch in w.lower() if str.isalpha(ch))
        yield {
            "text": word,
            "doc_id": row["doc_id"]
        }


def word_count_reduce(key, rows):
    res = dict(key)
    res["count"] = sum(1 for _ in rows)
    yield res


def build_word_count_graph(input_stream):
    graph = ComputeGraph(source=input_stream)
    graph.add_map(split_word_map)
    graph.add_sort(sort_by="text")
    graph.add_reduce(word_count_reduce, reduce_by="text")
    graph.add_sort(sort_by=("count", "text"))
    return graph


def count_docs_fold(rows):
    return {"total_docs": sum(1 for _ in rows)}


def idf_counter(key, rows):
    res = dict(key)
    freq = 0
    for row in rows:
        freq += 1
    if freq:
        total_docs = row["total_docs"]
        res["idf"] = log(total_docs / freq)
        yield res


def tf_counter(key, rows):
    word_count = defaultdict(int)
    for row in rows:
        word_count[row["text"]] += 1

    total = sum(word_count.values())
    for w, count in word_count.items():
        yield {
            "doc_id": row["doc_id"],
            "text": w,
            "tf": count / total
        }


def invert_index(key, rows):
    rows = heapq.nlargest(3, rows, key=lambda row: row["tf"] * row["idf"])
    for row in rows:
        row["tf_idf"] = row["tf"] * row["idf"]
        del row["tf"]
        del row["idf"]
    for row in sorted(rows, key=lambda r: r["tf_idf"], reverse=True):
        yield row


def build_inverted_index_graph(input_stream):
    split_word_graph = ComputeGraph(source=input_stream)
    split_word_graph.add_map(split_word_map)

    count_docs_graph = ComputeGraph(source=input_stream)
    count_docs_graph.add_fold(count_docs_fold)

    idf_graph = ComputeGraph(source=split_word_graph)
    idf_graph.add_sort(sort_by=("doc_id", "text"))
    idf_graph.add_reduce(word_count_reduce, reduce_by=("doc_id", "text"))

    idf_graph.add_join(on=count_docs_graph, strategy="inner")
    idf_graph.add_sort(sort_by="text")
    idf_graph.add_reduce(idf_counter, reduce_by="text")

    calc_index = ComputeGraph(source=split_word_graph)
    calc_index.add_sort(sort_by="doc_id")
    calc_index.add_reduce(tf_counter, reduce_by="doc_id")

    calc_index.add_sort(sort_by="text")
    calc_index.add_join(on=idf_graph, join_by="text", strategy="inner")
    calc_index.add_sort("text")
    calc_index.add_reduce(invert_index, reduce_by="text")
    calc_index.add_sort("text")

    return calc_index


def doc_filter_reducer(key, rows):
    word = key["text"]
    if len(word) < 4:
        return
    docs_with_1 = list()
    docs_with_2 = list()
    total_count = 0
    for row in rows:
        doc_id = row["doc_id"]
        if doc_id in docs_with_2:
            pass
        elif doc_id in docs_with_1:
            docs_with_1.remove(doc_id)
            docs_with_2.append(doc_id)
        else:
            docs_with_1.append(doc_id)
        total_count += 1
    if len(docs_with_2) == row["total_docs"]:
        yield {
            "text": word,
            "total_count": total_count
        }


def pmi_reducer(key, rows):
    doc_id = key["doc_id"]

    word_count = defaultdict(int)
    for row in rows:
        word_count[row["text"]] += 1. / row["total_count"]

    words = sorted(word_count.items(), key=lambda word: word[1], reverse=True)

    for w, pmi in words:
        yield {
            "doc_id": doc_id,
            "text": w,
            "pmi": pmi
        }


def build_pmi_graph(input_stream):
    split_word_graph = ComputeGraph(source=input_stream)
    split_word_graph.add_map(split_word_map)

    count_docs_graph = ComputeGraph(source=input_stream)
    count_docs_graph.add_fold(count_docs_fold)

    doc_filter_graph = ComputeGraph(source=split_word_graph)
    doc_filter_graph.add_join(on=count_docs_graph, strategy="inner")
    doc_filter_graph.add_sort(sort_by="text")
    doc_filter_graph.add_reduce(doc_filter_reducer, reduce_by="text")

    calc_pmi = ComputeGraph(source=split_word_graph)
    calc_pmi.add_sort(sort_by="text")
    calc_pmi.add_join(on=doc_filter_graph, join_by="text", strategy="inner")
    calc_pmi.add_sort(sort_by="doc_id")
    calc_pmi.add_reduce(pmi_reducer, reduce_by="doc_id")

    return calc_pmi


def distance(start, end):
    radius = 6371
    dlon = radians(start[0]) - radians(end[0])
    dlat = radians(start[1]) - radians(end[1])
    sq_sum = sin(dlat / 2) ** 2 + cos(radians(start[0])) * cos(radians(start[1])) * sin(dlon / 2) ** 2
    return 2 * atan2(sqrt(sq_sum), sqrt(1 - sq_sum)) * radius


def edges_mapper(row):
    res = dict()
    res["edge_id"] = row.get("edge_id")
    start, end = row.get("start"), row.get("end")
    res["length"] = distance(start, end)
    yield res


def times_mapper(row):
    mask = "%Y%m%dT%H%M%S.%f"
    enter = datetime.datetime.strptime(row["enter_time"], mask)
    leave = datetime.datetime.strptime(row["leave_time"], mask)
    res = dict()
    res["edge_id"] = row.get("edge_id")
    res["time"] = (leave - enter).total_seconds() / 3600.
    res["hour"] = enter.hour
    res["weekday"] = enter.strftime('%a')
    yield res


def times_reducer(key, rows):
    res = key
    total_time = 0
    total_dist = 0
    for row in rows:
        total_dist += row["length"]
        total_time += row["time"]
    res["speed"] = total_dist / total_time
    yield res


def build_yandex_maps_graph():
    edges = ComputeGraph(source="edges_input")
    edges.add_map(edges_mapper)
    edges.add_sort(sort_by="edge_id")

    times = ComputeGraph(source="times_input")
    times.add_map(times_mapper)
    times.add_sort(sort_by="edge_id")
    times.add_join(on=edges, join_by="edge_id")
    times.add_sort(sort_by=("weekday", "hour"))
    times.add_reduce(times_reducer, reduce_by=("weekday", "hour"))
    times.add_sort(sort_by="hour")
    return times
