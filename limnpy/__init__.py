import csv, yaml, json
import os, logging
import datetime
from operator import itemgetter
from collections import Sequence, MutableSequence
import codecs
import colorbrewer
import itertools
import pandas as pd
import pprint
import copy

logger = logging.getLogger(__name__)

limn_date_fmt = '%Y/%m/%d'

class DataSource(object):
    """
    This class represents a limn datasource including its associated datafile.
    The constructor takes in the datasource id, name and the actual data.
    Once the datasource hsa been constructed, you can add or modify the __data__
    member which is just a pandas.DataFrame object or the __source__ dictionary
    which maps directly to the datasource YAML file required by limn.  After
    modifying the __data__ and __source__ to your liking (or not at all), calling
    write() will produce both the YAML and csv files required by limn in the appropriate
    directories ({basedir}/datafiles, {basedir}/datasources).  You can also
    create a graph from the datasource including all of its columns or only a
    subset by calling the write_graph() method.  This 

    Examples:

        >>> import limnpy, datetime
        >>> rows = [[datetime.date(2012, 9, 1), 1, 2],
        ...          [datetime.date(2012, 10, 1), 7, 9]]
        >>> ds = limnpy.DataSource('test_source', 'Test Source', rows, labels=['date', 'x', 'y'])
        >>> ds.write(basedir='doctest_tmp')
        >>> hash(open('doctest_tmp/datasources/test_source.yaml').read())
        -6541337400615626104
        >>> hash(open('doctest_tmp/datafiles/test_source.csv').read())
        -9093178411304175629

        >>> ds.write_graph(basedir='doctest_tmp') # plot all columns
        >>> hash(open('doctest_tmp/graphs/test_source.json').read())
        -6063105524197132446

        >>> ds.__source__['id'] = 'test_source_just_x'
        >>> ds.write_graph(metric_ids=['x'], basedir='doctest_tmp') # just plot x
        >>> hash(open('doctest_tmp/graphs/test_source_just_x.json').read())
        4932947664539037265

        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...          {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9}]
        >>> ds = limnpy.DataSource('test_source', 'Test Source', rows)
        >>> ds.write(basedir='doctest_tmp')
        >>> hash(open('doctest_tmp/datasources/test_source.yaml').read())
        -6541337400615626104

        >>> rows = {'date' : [datetime.date(2012, 9, 1), datetime.date(2012, 10, 1)], 'x' : [1, 7], 'y' : [2, 9]}
        >>> ds = limnpy.DataSource('test_source', 'Test Source', rows)
        >>> ds.write(basedir='doctest_tmp')
        >>> hash(open('doctest_tmp/datasources/test_source.yaml').read())
        -6541337400615626104

    """

    default_source = {}
    default_source['id'] = None
    default_source['name'] = None
    default_source['shortName'] = default_source['name']
    default_source['format'] = 'csv'
    default_source['url'] = None
    
    timespan = {}
    timespan['start'] = None
    timespan['end'] = None
    timespan['step'] = '1d'
    default_source['timespan'] = timespan
    
    columns = {}
    columns['types'] = None
    columns['labels'] = None
    default_source['columns'] = columns

    default_source['chart'] = {'chartType' : 'dygraphs'}    

    def __init__(self, limn_id, limn_name, data, labels=None, types=None, date_key='date'):
        """
        Constructs a Python representation of Limn (github.com/wikimedia/limn) datasource
        including both the metadata YAML file (known as a datasource) and the associated csv
        file containing the actual content (known as a datafile).
        Args:
            limn_id   (str)       : the id used to uniquely identify this datasource in limn
            limn_name (str)       : the name which will be displayed to users for this datasource
            data      (anything pandas.DataFrame accepts) :
                                    the actual data associated with this datasource.  Can take
                                    any format accepted by the pandas.DataFrame constructor.
                                    which includes things like list of lists, list of dicts, dict
                                    mapping column names to lists, numpy ndarrays.  See:
                                    http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe
                                    for more information.
            labels    (list)      : the labels corresponding to the data "columns".  Not required
                                    if the data object
            types     (list)      : the javascript/limn types associated with each column of the csv file
                                    mostly this just means `int` and `date`
            date_key  (str)       : name of the column to be used as the date column.  Defaults to 'date'
        """

        self.date_key = date_key
        self.__source__ = copy.deepcopy(DataSource.default_source)
        self.__source__['id'] = limn_id
        self.__source__['name'] = limn_name
        self.__source__['shortName'] = limn_name
        self.__source__['url'] = '/data/datafiles/' + limn_id + '.csv'
        self.__source__['columns']['types'] = types

        # NOTE: though we construct the __data__ member here, we allow the possibility
        # that it will change before we write, so all derived fields get set in write()
        try:
            self.__data__ = pd.DataFrame(data)
        except:
            logger.exception('Error constructing DataFrame from data: %s.  See pandas.DataFrame documentation for help', data)
        if list(self.__data__.columns) == range(len(self.__data__.columns) or labels is not None):
            logger.debug('labels were not set by Pandas, setting manually')
            # this means the `data` object didn't include labels
            if labels is not None:
                self.__data__.rename(columns=dict(enumerate(labels)), inplace=True)
            else:
                raise ValueError('`data` does not contain label information, column names must be passed in with the labels arg')
        assert self.date_key in self.__data__.columns, 'date_key: `%s` must be in column labels: %s' % (date_key, list(self.__data__.columns))
        try:
            self.__data__.set_index(self.date_key, inplace=True)
        except:
            print 'error resetting index because self.__data__.columns=%s' % self.__data__.columns
        self.infer() # can't hurt to infer now. this way we can make graphs before writing the datasource


    def infer(self):
        """
        Infers the required metadata from the data if possible.  This is distinct
        from the __init__ routine so that the user can change the data after constructing
        it and the meta data will accurately reflect any added data
        """
        # parse dates, sort, and format
        self.__data__.index = pd.to_datetime(self.__data__.index)
        self.__data__.sort()
        self.__data__.index = self.__data__.index\
                                           .astype(pd.lib.Timestamp)\
                                           .map(lambda ts : ts.strftime(limn_date_fmt))
        
        # fill in data dependent keys
        self.__source__['columns']['labels'] = ['date'] + list(self.__data__.columns)
        self.__source__['timespan']['start'] = self.__data__.index[0]
        self.__source__['timespan']['end'] = self.__data__.index[-1]
        if self.__source__['columns']['types'] is None:
            self.__source__['columns']['types'] = ['date'] + ['int'] * len(self.__data__.columns)


    def write(self, basedir='.'):
        """
        Infers metadata from data and writes datasource csv and YAML files
        to {basedir}/datasources and {basedir}/datafiles respectively
        Args:
            basedir (str) : specifies the directory in which to place the datasources
                            and datafiles directories
        """
        
        self.infer()

        # make dirs and write files
        df_dir = os.path.join(basedir, 'datafiles')
        df_path = os.path.join(df_dir, self.__source__['id'] + '.csv')
        logger.debug('writing datafile to: %s', df_path)
        if not os.path.exists(df_dir):
            os.makedirs(df_dir)
        self.__data__.to_csv(df_path, index_label='date', encoding='utf-8')

        logger.debug(pprint.pformat(self.__source__))

        ds_dir = os.path.join(basedir, 'datasources')
        ds_path = os.path.join(ds_dir, self.__source__['id'] + '.yaml')
        logger.debug('writing datasource to: %s', ds_path)
        if not os.path.exists(ds_dir):
            os.makedirs(ds_dir)
        yaml_f = open(ds_path, 'w')

        # the canonical=True arg keeps pyyaml from turning the str "y" (and others) into True
        yaml.safe_dump(self.__source__, yaml_f, default_flow_style=False, canonical=True)
        yaml_f.close()
        self.wrote = True


    def __repr__(self):
        return pprint.pformat(vars(self))


    def get_graph(self, metric_ids=None):
        """ Returns a limnpy.Graph object with each of the (selected) datasource's columns """
        self.infer()

        if metric_ids is not None:
            metric_ids = list(itertools.izip(itertools.repeat(self.__source__['id']), metric_ids))
            logger.debug(metric_ids)
        return Graph(self.__source__['id'], self.__source__['name'], [self], metric_ids)


    def write_graph(self, metric_ids=None, basedir='.'):
        """
        Writes a graph with the (selected) datasource columns to the graphs dir in the 
        optionally specified basedir (defaults to .)"""
        g = self.get_graph(metric_ids)
        g.write(basedir)


class Graph(object):
    """
    Represents a limn compatible graph with the provided options.

        >>> import limnpy, datetime
        >>> rows1 = [[datetime.date(2012, 9, 1), 1, 2],
        ...         [datetime.date(2012, 10, 1), 7, 9]]
        >>> s1 = limnpy.DataSource('source1', 'Source 1', rows1, labels=['date', 'x', 'y'])
        >>> s1.write(basedir='doctest_tmp')
        >>> rows2 = [[datetime.date(2012, 9, 1), 19, 22],
        ...         [datetime.date(2012, 10, 1), 27, 29]]
        >>> s2 = limnpy.DataSource('source2', 'Source 2', rows2, labels=['date', 'x', 'y'])
        >>> s2.write(basedir='doctest_tmp')
        >>> g = limnpy.Graph('my_first_autograph', 'My First Autograph', [s1, s2], [('source1', 'x'), ('source2', 'y')])
        >>> g.write(basedir='doctest_tmp')
        >>> hash(open('doctest_tmp/graphs/my_first_autograph.json').read())
        -7062740022070187030

    or just pass in the sources and a graph will be constructed containing all of the columns
    in all of the sources

        >>> rows = [[datetime.date(2012, 9, 1), 1, 2],
        ...         [datetime.date(2012, 10, 1), 7, 9]]
        >>> s1 = limnpy.DataSource('source1', 'Source 1', rows, labels=['date', 'x', 'y'])
        >>> g = limnpy.Graph('my_first_default_autograph', 'My First Default Autograph', [s1])
        >>> g.write(basedir='doctest_tmp')
        >>> hash(open('doctest_tmp/graphs/my_first_default_autograph.json').read())
        5355513043120506668


    """
    
    default_graph = {
        "name": None,
        "notes": "",
        "callout": {
            "enabled": True,
            "metric_idx": 0,
            "label": ""
            },
        "slug": None,
        "width": "auto",
        "parents": ["root"],
        "result": "ok",
        "id": None,
        "chartType": "dygraphs",
        "height": 320,
        "data": {
            "metrics": [],
            "palette": None,
            "lines": []
            },
        "options": {
            "strokeWidth": 2,
            },
        "desc": ""
        }


    def __init__(self, id, title, sources, metric_ids=None, slug=None):
        """
        Construct a Python object representing a limn graph.
        Args:
            id         (str)   : graph id which uniquely identifies this graph for use in dashboards and such
            title      (str)   : title which will be displayed above graph
            sources    (list)  : list of limnpy.DataSource objects from which to construct the graph
        Kwargs:
            metric_ids (list)  : list of tuples (datasource_id, column_name) to plot if None will
                                 plot all of the columns from all of the datasources
            slug       (str)   : slug used to identify the graph by url (via {domain}/graphs/slug)
                                 defaults to the value of `id`
        """
        self.__graph__ = Graph.default_graph

        self.__graph__['id'] = id
        self.__graph__['name'] = title
        if slug is None:
            self.__graph__['slug'] = id
        else:
            self.__graph__['slug'] = slug

        if metric_ids is None:
            metric_ids = []
            for source in sources:
                labels = set(source.__source__['columns']['labels']) - set(['date'])
                source_id_repeat = itertools.repeat(source.__source__['id'])
                metric_ids.extend(list(itertools.izip(source_id_repeat,labels)))

        color_map = self.get_color_map(len(metric_ids))

        metrics = []
        source_dict = {source.__source__['id'] : source for source in sources}
        for i, (source_id, col_key) in enumerate(metric_ids):
            source = source_dict[source_id]
            try:
                m = Graph.get_metric(source, i, col_key, color_map[i])
                metrics.append(m)
            except ValueError:
                logger.warning('Could not find column label: %s in datasource: %s', col_key, source.__source__['id'])
        self.__graph__['data']['metrics'] = metrics
    

    def write(self, basedir='.'):
        """
        writes graph JSON file to {basedir}/graphs.
        Args:
            basedir (str) : specifies the directory in which to place the graphs
                            will create the graphs directory if it doesn not already
                            exist
        """

        graphdir = os.path.join(basedir, 'graphs')
        if not os.path.isdir(graphdir):
            os.mkdir(graphdir)
        graph_fn = os.path.join(graphdir, self.__graph__['id'] + '.json')

        graph_f = codecs.open(graph_fn, encoding='utf-8', mode='w')
        json.dump(self.__graph__, graph_f, indent=2)
        graph_f.close()
    

    @classmethod
    def get_color_map(cls, n):
        """ get colorspace based on number of metrics using colorbrewer """
        family = colorbrewer.Set2
        if n == 2:
            color_map = [family[3][0], family[3][2]]
        if n < 3:
            color_map = family[3][:n]
        elif n >= max(family.keys()):
            logger.warning('too many metrics, looping over color space')
            color_map = itertools.cycle(family[11])
            color_map = list(itertools.islice(color_map, None, n))
        else:
            color_map = family[n]
        str_color_map = ['rgb(%d,%d,%d)' % color_tuple for color_tuple in color_map]
        return str_color_map
    

    @classmethod
    def get_metric(cls, source, index, col_key, color=None):
        """ constructs a limn-compatible dictionary represnting a metric """
        col_idx = source.__source__['columns']['labels'].index(col_key)
        metric = {
            "index": index,
            "scale": 1,
            "timespan": {
                "start": None,
                "step": None,
                "end": None
                },
            "color": color,
            "format_axis": None,
            "label": col_key,
            "disabled": False,
            "visible": True,
            "format_value": None,
            "transforms": [],
            "source_id": source.__source__['id'],
            "chartType": None,
            "type": "int",
            "source_col": col_idx
            }
        return metric
