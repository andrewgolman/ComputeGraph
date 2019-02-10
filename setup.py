#!/usr/bin/env python

from setuptools import setup

packages = {
    "compgraph": "compgraph/",
    "compgraph.src": "compgraph/src",
}

setup(
    name="compgraph",
    version="1.2",
    description="Implementation of a graph for MapReduce computations on streams/iterables",
    author="Andrew Golman",
    author_email="andrewsgolman@gmail.com",
    requires=[],
    packages=packages,
    package_dir=packages,
)
