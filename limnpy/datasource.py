import csv, yaml, json
import os, logging
import datetime
from operator import itemgetter
from collections import Sequence, MutableSequence
import codecs
#import colorbrewer
import itertools
import pandas as pd
import pprint
import copy

from graph import Graph

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataSource(object):
    """
    This class represents a limn datasource including its associated datafile.
    The constructor takes in the datasource id, name and the actual data.
    Once the datasource has been constructed, you can add or modify the __data__
    member which is just a pandas.DataFrame object or the __source__ dictionary
    which maps directly to the datasource YAML file required by limn.  After
    modifying the __data__ and __source__ to your liking (or not at all), calling
    write() will produce both the YAML and csv files required by limn in the appropriate
    directories ({basedir}/datafiles, {basedir}/datasources).  You can also
    create a graph from the datasource including all of its columns or only a
    subset by calling the write_graph() method.

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

    default_source = {
        'id' : None,
        'slug' : None,
        'format' : 'csv',
        'type' : 'timeseries',
        'url' : None,
        'name' : '',
        'shortName' : '',
        'desc' : '',
        'notes' : '',
        'columns' : { 
            'types' : None,
            'labels' : None
        },
        'timespan' : {
            'start' : None,
            'end' : None,
            'step' : '1d'
        }
    }
    
    def __init__(self, limn_id, limn_name, data, labels=None, types=None, date_key='date', date_fmt='%Y/%m/%d'):
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
            date_fmt  (str)       : date format of the date column.
        """

        self.date_key = date_key
        self.date_fmt = date_fmt
        self.__source__ = copy.deepcopy(DataSource.default_source)
        self.__source__['id'] = limn_id
        self.__source__['name'] = limn_name
        self.__source__['shortName'] = limn_name
        self.__source__['url'] = '/data/datafiles/' + limn_id + '.csv'
        self.__source__['columns']['types'] = types

        # NOTE: though we construct the __data__ member here, we allow the possibility
        # that it will change before we write, so all derived fields get set in infer() which is called by write()
        try:
            self.__data__ = pd.DataFrame(copy.deepcopy(data))
        except:
            raise ValueError('Error constructing DataFrame from data: %s.  See pandas.DataFrame documentation for help' % data)
        # check whether columns are not named or the labels field has been passed in
        if list(self.__data__.columns) == range(len(self.__data__.columns) or labels is not None):
            logger.debug('labels were not set by Pandas, setting manually')
            # this means the `data` object didn't include column labels
            if labels is not None:
                self.__data__.rename(columns=dict(enumerate(labels)), inplace=True)
            else:
                raise ValueError('`data` does not contain label information, column names must be passed in with the `labels` arg')

        if not isinstance(self.__data__.index, pd.tseries.index.DatetimeIndex):
            logger.debug('dealing with a DataFrame instance that does NOT have a datetime index.  type(index)=%s', type(self.__data__.index))
            # if `data` is just another pd.DataFrame from a DataSource or datetime-indexed, don't to set index
            if self.date_key not in self.__data__.columns:
                raise ValueError('date_key: `%s` must be in column labels: %s\ntype(self.__data__.index): %s, self.__data__.index: %s' %
                        (date_key, list(self.__data__.columns), type(self.__data__.index), self.__data__.index))
            try:
                self.__data__.set_index(self.date_key, inplace=True)
            except:
                logger.exception('error resetting index because self.__data__.columns=%s', self.__data__.columns)
                raise ValueError('could not set_index because self.__data__.columns=%s', self.__data__.columns)
        self.infer() # can't hurt to infer now. this way we can make graphs before writing the datasource


    def infer(self):
        """
        Infers the required metadata from the data if possible.  This is distinct
        from the __init__ routine so that the user can change the data after constructing
        it and the meta data will accurately reflect any added data
        """
        # parse dates, sort, and format
        # logger.debug('entering infer with self.__data__:\n%s', self.__data__)
        self.__data__.index = pd.to_datetime(self.__data__.index)
        # logger.debug('converted index to timestamps.  self.__data__.index:\n%s', self.__data__.index)
        # logger.debug('id: %s', self.__source__['id'])
        # logger.debug('set index to be a datetime index. type(self.__data__.index) = %s', type(self.__data__.index))
        # logger.debug('id(self) = %s', id(self))
        self.__data__.sort()
        # logger.debug('columns: %s', self.__data__.columns)
        # logger.debug('reverse columns: %s', list(reversed(self.__data__.sum().argsort(order=True))))
        # self.__data__ = self.__data__[self.__data__.columns[list(reversed(self.__data__.sum().argsort(order=True)))]]
        logger.debug('self.__data__:\n%s', self.__data__)
        # self.__data__ = self.__data__.fillna(0) # leaving the NAs in until writing is better so that we can just write ''

        # fill in data dependent keys
        self.__source__['columns']['labels'] = ['date'] + list(self.__data__.columns)
        str_ind = self.__data__.index.astype(pd.lib.Timestamp).map(lambda ts : ts.strftime(self.date_fmt))
        if len(str_ind) > 0:
            self.__source__['timespan']['start'] = str_ind[0]
            self.__source__['timespan']['end'] = str_ind[-1]
        if self.__source__['columns']['types'] is None:
            self.__source__['columns']['types'] = ['date'] + ['int'] * len(self.__data__.columns)
        # logger.debug('exiting infer with self.__data__:\n%s', self.__data__)


    def write(self, basedir='.'):
        """
        Infers metadata from data and writes datasource csv and YAML files
        to {basedir}/datasources and {basedir}/datafiles respectively
        Args:
            basedir (str) : specifies the directory in which to place the datasources
                            and datafiles directories
        """
        
        self.infer()
        self.__data__.index = self.__data__.index\
                                           .astype(pd.lib.Timestamp)\
                                           .map(lambda ts : ts.strftime(self.date_fmt))

        # make dirs and write files
        df_dir = os.path.join(basedir, 'datafiles')
        df_path = os.path.join(df_dir, self.__source__['id'] + '.csv')
        logger.debug('writing datafile to: %s', df_path)
        if not os.path.exists(df_dir):
            os.makedirs(df_dir)
        self.__data__.to_csv(df_path, index_label='date', encoding='utf-8')

        logger.debug(pprint.pformat(self.__source__))

        ds_dir = os.path.join(basedir, 'datasources')
        ds_path = os.path.join(ds_dir, self.__source__['id'] + '.json')
        logger.debug('writing datasource to: %s', ds_path)
        if not os.path.exists(ds_dir):
            os.makedirs(ds_dir)
        json_f = open(ds_path, 'w')

        # the canonical=True arg keeps pyyaml from turning the str "y" (and others) into True
        json.dump(self.__source__, json_f, indent=4)
        json_f.close()
        self.wrote = True


    def __repr__(self):
        return pprint.pformat(vars(self))


    def get_graph(self, metric_ids=None):
        """
        Returns a limnpy.Graph object with each of the (selected) datasource's columns 
        Args:
            metric_ids (list(str)) :  a list of the datasource columns to use in the graph
        """
        self.infer()

        metric_ids = metric_ids if metric_ids is not None else self.__data__.columns
        g = Graph(self.__source__['id'], self.__source__['name'])
        for metric_id in metric_ids:
            g.add_metric(self, metric_id)
        return g


    def write_graph(self, metric_ids=None, basedir='.'):
        """
        Writes a graph with the (selected) datasource columns to the graphs dir in the 
        optionally specified basedir (defaults to .)
        Args:
            metric_ids (list(str)) :  a list of the datasource columns to use in the graph (defaults to all)
            basedir (str)          :  specifies the directory in which to place the datasources
                                      and datafiles directories (defaults to `.`)
        """
        g = self.get_graph(metric_ids)
        g.write(basedir)
        return g
