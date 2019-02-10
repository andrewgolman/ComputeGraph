#!/usr/bin/env python
import json
import argparse
from compgraph.compgraph.algorithms import build_yandex_maps_graph


def main():
    parser = argparse.ArgumentParser("Example solution for the Maps problem")
    parser.add_argument("graph_data")
    parser.add_argument("travel_times")
    args = vars(parser.parse_args())
    with open(args["graph_data"]) as graph_data_stream:
        with open(args["travel_times"]) as travel_times_stream:
            graph_data = iter(json.loads(row) for row in graph_data_stream)
            travel_times = iter(json.loads(row) for row in travel_times_stream)
            g = build_yandex_maps_graph()
            for row in g.run(edges_input=graph_data, times_input=travel_times):
                print(json.dumps(row))


if __name__ == "__main__":
    main()
