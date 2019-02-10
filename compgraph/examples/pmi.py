#!/usr/bin/env python

import json
import argparse
from compgraph.compgraph.algorithms import build_pmi_graph


def main():
    parser = argparse.ArgumentParser("Example solution for the PMI problem")
    parser.add_argument("docs")
    parser.add_argument("travel_times")
    args = vars(parser.parse_args())
    with open(args["docs"]) as docs_file:
        g = build_pmi_graph('docs')
        docs = iter(json.loads(row) for row in docs_file)
        for row in g.run(docs=docs):
            print(json.dumps(row))


if __name__ == "__main__":
    main()
