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
to run the tests, so you'll want to remove the `limnpy/datafiles/` and `limnpy/datasources` directories.

## Usage

`limnpy` offers a simple module-level function `write` which does everything at once:

````python
import limnpy, datetime
rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
limnpy.write('evan_test', 'Evan Test')
````
which should create the files `./datasources/evan_test.yaml` and `./datafiles/evan_test.csv`

`write` should really handle most cases, but if you want a little more control over the process
you can write each row individually and write the datasource file explicitly.  `limnpy` contains a class
called `DictWriter` whose interface is designed to emulate the `csv` module's `DictWriter` class.

````python
writer = limnpy.DictWriter('evan_test', "Evan's Test", keys=['date', 'x', 'y'])
for row in rows:
    writer.writerow(row)
writer.writesource()
````