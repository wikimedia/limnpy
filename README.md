limnpy
======

A library for creating [limn](https://github.com/wikimedia/limn) compatible datasources and datafiles

## Introduction

[`limn`](github.com/wikimedia/limn), a visualization tool for non-programmers, still requires at least one or two programmers
to generate the datasources which can be plotted.  In order to make a timeseries available for plotting with `limn`
you need to generate an appropriately formatted csv file (known as a `datafile`) as well as a valid YAML file with metadata
(known as a `datasource`). `limnpy` aims to solve the simple cases where you've prepared the timeseries data and
just want to write a `datafile` and `datasource` pair without worrying about the details.

## Installation

`limnpy` is packaged with setuptools, so you have some options, but I recommend

````
$ cd limnpy/
$ pip install -e .
````

run the tests the following command from inside the main directory:

````
$ python -m doctest limnpy/__init__.py
````

which shouldn't print anything if all goes well.  The only catch is that it does actually generate files in order
to run the tests, so you'll want to remove the `limnpy/doctest_tmp` directory after testing.

## Usage

To dump data to file simply construct an instance of a `limnpy.DataSource` and call its `write()` method.

````python
import limnpy, datetime
rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
ds = limnpy.DataSource('evan_test', 'Evan Test', rows)
ds.write()
````
which should create the files `./datasources/evan_test.yaml` and `./datafiles/evan_test.csv`.  To control the location
of these files, you can pass in the `basedir` argument to the `write()` method which directs limnpy to
place the YAML and csv files in `basedir/{datasources,datafiles,graphs}`.

````
ds.write(basedir='../in/a/directory/far/far/away/')
````

Just calling the constructor with the appropriate arguments should really handle most cases, 
but for everything else you can just direclty manipulate the `__source__` and `__data__` fields before calling `write()`.  The
`__source__` field is just a nested map/list object which directly maps to the YAML datasource file
and the `__data__` attribute returns a reference to the internal `pandas.DataFrame` object which
limnpy uses to store the data and labels.  This has the perk of making lots of standard data cleaning/tranforming taks
relatively easy (if you are familiar with `pandas`)

````
ds1 = limnpy.DataSource('scaled', 'Data Scaled by a factor of 1000', ds.__data__ * 1000)
combined = limnpy.DataSource('combined', 'Combined', pd.merge(ds.__data__, ds1.__data__))
````

### Acceptable Data Formats
Because `limnpy` uses `pandas.DataFrame` objects internally, it defers the parsing of the constructor's `data` argument 
to the `pandas.DataFrame` constructor.  This means that you can construct a `DataSource` object from whatever
format your data already exist in.  The only catch is that the `DataSource` needs to know the labels to use for
identify the date column and identifying the columns to the Limn graph maker.  So, if the datasource you pass in
does not represent the column names, you need to pass in a list of strings with the optional `labels` parameter

````
rows = [[datetime.date(2012, 9, 1), 1, 2],                                                                                                                                                   
        [datetime.date(2012, 10, 1), 7, 9]
ds = DataSource('id', 'Name', rows, labels=['date', 'x', 'y'])

rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},                                                                                                                              
        {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9}]
ds = DataSource('id', 'Name', rows)

rows = {'date' : [datetime.date(2012, 9, 1), datetime.date(2012, 10, 1)],
        'x' : [1, 7],
        'y' : [2, 9]}
ds = DataSource('id', 'Name', rows)

import pandas as pd
rows = pd.DataFrame(rows)
ds = DataSource('id', 'Name', rows)
````

### Graphs
Another common task is the automatic generation of a graph.  To construct a graph from a `limnpy.DataSource`
object, just call `ds.write_graph()`.  Or to specify a particular set of columns to plot from a datasource, call

````
ds.write_graph(['x'])
````

To make a graph which contains columns from more than one DataSeries, you can directly construct an instance of
`limnpy.Graph`, specify which columns to use from which `DataSource`s with a list of `(datasource_id, col_name)` tuples
and then calling its `write()` method.

````
rows1 = [[datetime.date(2012, 9, 1), 1, 2],                                                                                                                                                  
         [datetime.date(2012, 10, 1), 7, 9]]                                                                                                                                                  
s1 = limnpy.DataSource('source1', 'Source 1', rows1, labels=['date', 'x', 'y'])                                                                                                              
s1.write(basedir='doctest_tmp')                                                                                                                                                              
rows2 = [[datetime.date(2012, 9, 1), 19, 22],                                                                                                                                                
         [datetime.date(2012, 10, 1), 27, 29]]                                                                                                                                                
s2 = limnpy.DataSource('source2', 'Source 2', rows2, labels=['date', 'x', 'y'])                                                                                                              
s2.write(basedir='doctest_tmp')                                                                                                                                                              
g = limnpy.Graph('my_first_autograph', 'My First Autograph', [s1, s2], [('source1', 'x'), ('source2', 'y')])                                                                                 
g.write(basedir='doctest_tmp')                                      
````