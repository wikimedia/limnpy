limnpy
======

A library for creating [limn](https://github.com/wikimedia/limn) compatible datasources and datafiles

* [Introduction](#introdcution)
* [Installation](#installation)
* [Library Usage](#usage)
    * [Acceptable Data Formats](#acceptable-data-formats)
    * [Graphs](#graphs)
    * [Dashboards](#dashboards)
* [Command Line Utility](#command-line-utility)

## Introduction

[`limn`](github.com/wikimedia/limn), a visualization tool for non-programmers, still requires at least one or two programmers
to generate the datasources which can be plotted.  In order to make a timeseries available for plotting with `limn`
you need to generate an appropriately formatted csv file (known as a `datafile`) as well as a valid YAML file with metadata
(known as a `datasource`). `limnpy` aims to solve the simple cases where you've prepared the timeseries data and
just want to write a `datafile` and `datasource` pair without worrying about the details.

## Installation

`limnpy` is packaged with setuptools, so you have some options, but I recommend

````bash
$ cd limnpy/
$ pip install -e .
````

to run the tests the following command from inside the main directory:

````bash
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
ds = limnpy.DataSource('test_source', 'Test Source', rows)
ds.write()
````
which should create the files `./datasources/test_source.yaml` and `./datafiles/test_source.csv`.  To control the location
of these files, you can pass in the `basedir` argument to the `write()` method which directs limnpy to
place the YAML and csv files in `BASEDIR/{datasources,datafiles,graphs}`, creating any missing directories along the way.

````python
ds.write(basedir='../in/a/directory/far/far/away/')
````

Just calling the constructor with the appropriate arguments should really handle most cases, 
but for everything else you can just direclty manipulate the `__source__` and `__data__` fields before calling `write()`.  The
`__source__` field is just a nested `dict`/`list` object which directly maps to the YAML datasource file
and the `__data__` attribute returns a reference to the internal `pandas.DataFrame` object which
limnpy uses to store the data and labels.  This has the perk of making lots of standard data cleaning/tranforming taks
relatively easy (if you are familiar with `pandas`)

````python
ds_scaled = limnpy.DataSource('scaled', 'Data Scaled by a factor of 1000', ds.__data__ * 1000)
combined = limnpy.DataSource('combined', 'Combined', pd.merge(ds.__data__, ds_scaled.__data__))
````

### Acceptable Data Formats
Because `limnpy` uses `pandas.DataFrame` objects internally, it defers the parsing of the constructor's `data` argument 
to the `pandas.DataFrame` constructor.  This means that you can construct a `DataSource` object from whatever
format your data already exist in.  The only catch is that the `DataSource` needs to know the column labels for things like
figuring out which column contains the dates and eventually showing the Limn graph maker the name of the particular column
which they are plotting.  So, if the datasource you pass in does not represent the column names, 
you need to pass in a list of strings as the optional `labels` parameter

````python 
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

Lastly, because the date information requires some special handling, the DataSource needs to know which column
contains the dates.  By default a `DataSource` looks for a column labeled `date`,  but this can be overridden using
the `date_key` optional parameter:

````python
rows = [{'first_seen' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},                                                                                                                              
        {'first_seen' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9}]
ds = DataSource('id', 'Name', rows, date_key='first_seen')
````

### Graphs
Another common task is the automatic generation of a graph.  To construct a graph from a `limnpy.DataSource`
object containing all columns, just call `ds.write_graph()`.  Or, to specify a particular set of columns to
plot from a `DataSource`, call

````python
ds.write_graph(['x'])
````

To make a graph which contains columns from more than one DataSeries, you can directly construct an instance of
`limnpy.Graph`, specify which columns to use from which `DataSource`s with a list of `(datasource_id, col_name)` tuples
and then calling its `write()` method.

````python
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

### Dashboards
If you need to make a lot of dashboards, or don't want to worry about manually writing valid JSON, this tool is for you.  You can programmatically construct an instance of `limnpy.Dashboard` and then call its `write()` method to create the appropriate file.  First, call the constructor and specify the slug, title, and heading:

````python
db = limnpy.Dashboard('sobchak', 'Sobchak Security', 'Dashboard')
````

Which will construct a dashboard object which will be accessible at "your.domain.org/dashboards/sobchak". To add actual graphs you then call the dashboard object's `add_graph(name, graph_ids)` method, like this:

````python
db.add_tab('core', ['intruders'])
db.add_tab('core', [intruder_graph.__graph__['id']])
````

Alternatively, you can pass in a list of `dict`s to the Dashbaord contructor, which is formatted like this:

````python
[{'name' : 'core', 'graph_ids' : ['intruders_total', 'intruders_daily']},
 {'name' : 'ancillary', 'graph_ids' : ['false_alarms']}]
````

And to finally create the JSON file which the server will read, call `db.write(basedir)` to place the file in the appropriate subdirectory ('dashboards') of 'basedir'

## Command Line Utility

Installing `limnpy` also installs `limnify`, which is a highly customizable tool for taking turning a csv-like file into a limn-compatible datasource or graph which can be directly served by a limn installation.  In the simplest case, you just call

````
$ limnify my_data.csv
````

and it creates the files `./datafiles/my_data.csv` and `./datasources/my_data.yaml`.  But inevitably, your data will have it's own oddities.  `limnify` allows you to accomodate a variety of ways in which your data may need special care by using the various options described below.  But it's probably worth noting three main things about what `limnify` expects so you don't get a bunch of errors:
* it needs to know which column contains the date (just like the rest of `limnpy`)
* it needs to give each column a name so that end limn users to tell what is what
* it can accomodate "long" format data which it will pivot by summing the values in certain columns when grouped by other columns.

Here is the help page:

````
$ limnify --help
usage: limnify [-h] [--delim DELIM] [--header HEADER [HEADER ...]]
               [--datecol DATECOL] [--datefmt DATEFMT] [--pivot]
               [--metriccol METRICCOL] [--valcol VALCOL] [--basedir BASEDIR]
               [--name NAME [NAME ...]] [--id ID] [--write_graph WRITE_GRAPH]
               data

positional arguments:
  data                  name of file to be limnified

optional arguments:
  -h, --help            show this help message and exit
  --delim DELIM         delim to use for input file (default: ,)
  --header HEADER [HEADER ...]
                        this is a space separated list of names to use as the
                        header rowIf your data doesn't already have a header
                        row you will need to pass in a list of names to use,
                        otherwise itwill assume the first row is a header and
                        then produce confusing data sources. Remember, these
                        names will bedisplayed in the graph editing interface
                        (default: None)
  --datecol DATECOL     the date column name or index--required if it is
                        different from `date` (default: 0)
  --datefmt DATEFMT     format to use with datetime.strptime, default uses
                        dateutil.parser.parse (default: None)
  --pivot               whether to try and pivot the data (only supports sum
                        aggregation for now) (default: False)
  --metriccol METRICCOL
                        the column name or index to use for creating the
                        column (metric) names when pivoting (default: 1)
  --valcol VALCOL       the column in which to find the actual data to be
                        plotted when pivoting (default: 2)
  --basedir BASEDIR     directory in which to place the output datasources,
                        datafiles and graphs directories (default: .)
  --name NAME [NAME ...]
                        name of datasource which will be displayed in the UI
                        (default: None)
  --id ID               the slug / id used to uniquely identify the datasource
                        within a limn installation (default: None)
  --write_graph WRITE_GRAPH
                        whether to write a graph file containing all columns
                        from the datasource (default: False)
````
