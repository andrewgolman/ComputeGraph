## Compgraph

Compgraph is a Python library for performing
MapReduce computations with Python streams.

#### Installation

`git clone https://github.com/andrewgolman/compgraph.git` (didn't published that yet, can I?)

`cd compgraph`

`pip install compgraph` 

#### Available operations


1. **Map** — calls a generator on every row of the input table.
Formes a new table from all rows yielded from a mapper.

    Mapper guarantees returning sorted table if an input table was sorted.
    
    Mapper function should be a generator and take one argument, row of the 
    table as a dict. It may yield any number of rows. Examples:
          
    ```python
    def mapper(row):
        """filters out all rows with a negative or not defined "value" field,  
        increments "value" field of the others"""
        if row.get("value") > 0:
            row["value"] += 1
            yield row
    ```
        
    ```python
    def tokenizer_mapper(r):
     """
        splits rows with 'text' field into set of rows with 'token' field
       (one for every occurence of every word in text)
     """
    
     tokens = r['text'].split()
    
     for token in tokens:
       yield {
         'doc_id' : r['doc_id'],
         'word' : token,
       }
    ```

1. **Reduce** — call a generator on every set of rows with the same key (set
of columns). Input table should be sorted by the operation key.

    Reducer guarantees returning sorted table.
    
    Reducer function should be a generator and take two arguments: 
    dict with the values of a key that is being processed,
    and iterable of rows as dicts. Example:

    ```python
    def word_count_reducer(key, rows):
    """This reducer counts occurences of a word in the input table"""
        result = key
        result["count"] = len(rows)
        yield result
    ```
    ```python
   def term_frequency_reducer(records):
       """calculates term frequency for every word in doc_id"""
       word_count = Counter()

       for r in records:
           word_count[r['word']] += 1

       total  = sum(word_count.values)
       for w, count in word_count.items():
           yield {
               'doc_id' : r['doc_id'],
               'word' : w,
               'tf' : count / total
           }
   ```

1. **Fold** — fold the table into one row.
    
    Folder should be a function, take one argument and return dict. Example:
    
    ```python
    def folder(rows):
        """This folder sums column "value" from the table"""
        return {
            "name": "my_table",
            "sum": sum([row["value"] for row in rows])
        }
    ```
1. **Sort** — sort a table lexicographically by a given list of columns.  ​

1. **Join** — join two tables on the given key. 
**Both** input tables should be sorted by the operation key.


#### Usage

1. Create a compgraph object:

    `mygraph = compgraph.ComputeGraph(source=my_source)`

    `my_source` can be:

    - a name of the parameter to be passed to the run method

    - a `ComputeGraph` object

1. Add graph structure:

    - `mygraph.add_map(mapper=my_mapper)`.
    Add a map operation with mapper my_mapper
    
    - `mygraph.add_reducer(reducer=my_reducer,
    reduce_by=(column1, column2))`. 
    Add a reduce operation with reducer my_reducer,
    reduce by column in reduce_by - can be iterable or 
    a name of a column.
    
    - `mygraph.add_fold(folder=my_folder)`
    
    - `mygraph.add_sort(sort_by=column1)`.
    `Sort_by` can be iterable or a name of a column

    - `mygraph.add_join(on=another_graph, join_by=column1, 
    strategy="innner")`.
    `Join_by` can be iterable, a name of a column. May be an empty
     tuple, which will lead to a cross join.
     `Strategy` describes a type of a join: `inner`, `left`, `right`
     or `outer` (full outer join).

1. Create operation functions/generators: mappers, reducers and 
folders. (see available operations)

1. Start calculations, passing to `run` method 
sources for every dependency graph for your graph. All graphs will
be ran exactly once.

`mygraph.run(my_source=some_iterator)`

Sources can be iterables. You can also pass a `ComputeGraph` objects as sources,
but *no execution order* is guaranteed in this case for those graphs.

Same input names might be used for different graphs. In this case
they will get the same input.

#### Example

Classical wordcount problem: for every word in a corpus
count its occurrences.

```
graph = compgraph.ComputeGraph(source=input_stream)
graph.add_map(split_text_mapper)
graph.add_sort(sort_by="word")
graph.add_reduce(word_count_reducer, reduce_by="word")
graph.add_sort(sort_by=("count", "text"))
graph.run(input_stream=some_iterator)
```
        
        
#### More examples
    
- `examples/problems.md` - descriptions of the problems

- `examples/*.py` - their solutions with `ComputeGraph`. Scripts take data
 from `resource` and write output to stdout, so
you might want to redirect it to files.

- `tests/test_algorithms.py` - usage of algorithms from `examples/*.py`
