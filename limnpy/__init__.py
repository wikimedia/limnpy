import csv, yaml
import os, logging
import datetime
from operator import itemgetter
from collections import MutableSequence

limn_date_fmt = '%Y/%m/%d'

def write(id, name, keys, rows, **kwargs):
    """
    Intsance level convience method for carrying out all of the steps
    necessary to write a datafile and datasource

        >>> import limnpy, datetime
        >>> rows = [[datetime.date(2012, 9, 1), 1, 2],
        ...         [datetime.date(2012, 10, 1), 7, 9]]
        >>> limnpy.write('write_test', 'Write Test', rows, ['date', 'x', 'y'])
        >>> print open('./datasources/write_test.yaml').read().strip()
        chart:
          chartType: dygraphs
        columns:
          labels:
          - date
          - x
          - y
          types:
          - date
          - int
          - int
        format: csv
        id: write_test
        name: Write Test
        shortName: Write Test
        timespan:
          end: 2012-10-01
          start: 2012-09-01
          step: 1d
        url: /data/datafiles/write_test.csv
        >>> print open('./datafiles/write_test.csv').read().strip() # doctest: +NORMALIZE_WHITESPACE
        date,x,y
        2012/09/01,1,2
        2012/10/01,7,9
        >>>
    """
    writer = Writer(id, name, keys, **kwargs)
    writer.writerows(rows)
    writer.writesource()


def writedicts(id, name, rows, **kwargs):
    """
    Intsance level convience method for carrying out all of the steps
    necessary to write a datafile and datasource

        >>> import limnpy, datetime
        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...         {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
        >>> limnpy.writedicts('writedicts_test', "Write Dicts Test", rows)
        >>> print open('./datasources/writedicts_test.yaml').read().strip()
        chart:
          chartType: dygraphs
        columns:
          labels:
          - date
          - x
          - y
          types:
          - date
          - int
          - int
        format: csv
        id: writedicts_test
        name: Write Dicts Test
        shortName: Write Dicts Test
        timespan:
          end: 2012-10-01
          start: 2012-09-01
          step: 1d
        url: /data/datafiles/writedicts_test.csv
        >>> print open('./datafiles/writedicts_test.csv').read().strip() # doctest: +NORMALIZE_WHITESPACE
        date,x,y
        2012/09/01,1,2
        2012/10/01,7,9
        >>>
    """
    dwriter = DictWriter(id, name, **kwargs)
    dwriter.writerows(rows)
    dwriter.writesource()


class Writer(object):

    def __init__(self, 
                 id, 
                 name,
                 keys,
                 date_key=0,
                 basedir='.',
                 types=None):
        self.id = id
        self.name = name
        self.keys = keys
        self.date_key = date_key

        # will be populate by actually writing rows
        self.start = None
        self.end = None
        self.writer = None

        # check that output directories exist
        self.datafile_dir = os.path.join(basedir, 'datafiles')
        if not os.path.isdir(self.datafile_dir):
            os.mkdir(self.datafile_dir)
        self.datasource_dir = os.path.join(basedir, 'datasources')
        if not os.path.isdir(self.datasource_dir):
            os.mkdir(self.datasource_dir)
        self.csv_name = '%s.csv' % self.id
        self.yaml_name = '%s.yaml' % self.id

        # sets up a writer instance if we can
        if self.keys is not None:
            self.init_keys()


    def init_keys(self):
        if not self.writer:
            csv_path = os.path.join(self.datafile_dir, self.csv_name)
            self.csv_file = open(csv_path, 'w')
            self.writer = csv.writer(self.csv_file)
            self.writer.writerow(self.keys)


    def writerows(self, rows):
        rows = sorted(rows, key=itemgetter(self.date_key))
        for row in rows:
            self.writerow(row)
        self.flush()


    def writerow(self, row):
        if self.start is None or row[self.date_key] < self.start:
            self.start = row[self.date_key]
        if self.end is None or row[self.date_key] > self.end:
            self.end = row[self.date_key]
        if not isinstance(row[self.date_key], basestring):
            if not isinstance(row, MutableSequence):
                row = list(row)
            row[self.date_key] = row[self.date_key].strftime(limn_date_fmt)
        self.writer.writerow(row)

    def flush(self):
        self.csv_file.flush()

    def writesource(self):
        assert self.writer, 'no rows have been written. cannot write datasource'

        self.types = ['date'] + ['int']*(len(self.keys) - 1)

        meta = {}
        meta['id'] = self.id
        meta['name'] = self.name
        meta['shortName'] = meta['name']
        meta['format'] = 'csv'
        meta['url'] = '/data/datafiles/' + self.csv_name

        timespan = {}
        timespan['start'] = self.start
        timespan['end'] = self.end
        #timespan['step'] = '1mo'
        timespan['step'] = '1d'
        meta['timespan'] = timespan

        columns = {}
        columns['types'] = self.types
        columns['labels'] = self.keys
        meta['columns'] = columns

        meta['chart'] = {'chartType' : 'dygraphs'}

        yaml_path = os.path.join(self.datasource_dir, self.yaml_name)
        fyaml = open(yaml_path, 'w')
        fyaml.write(yaml.safe_dump(meta, default_flow_style=False))
        fyaml.close()


class DictWriter(Writer):
    """
    This class is a tool for writing limn compatible 'datasources' and 'datafiles'. It 
    emulates the csv.DictWriter class in that it provides a way to write a collection
    of dicts to file, where each dict represents a 'row' in the traditional sense.  Like 
    DictWriter, because dicts are unordered, the constructor takes in an ordered collection
    of keys to determine the ordering of the columns in the output file.  The constructor also 
    takes in a variety of limn-specific parameters

    parameters:
      keys      : list of keys used to extract the ordering of columns from each dict
      id        : the datasource unique id which will be used as a unique id by limn
      name      : the human-readable datasource name which will be displayed in the browser
      basedir   : the diretory in which to optionally create the `datasources`, and `datafiles`
                  directories into which the datasoures and datafiles themselves will be 
                  respectively added
      types     : list of types or javascript type strings correponding to each key given by 
                  the keys argument. If the arg is omitted the types will be set to 
                     ['date', 'int', 'int', ...]

    here's a simple example (set up for doctest) which shows a more granular way of controlling
    the construction of the limn files

        >>> import limnpy, datetime
        >>> writer = limnpy.DictWriter('dictwriter_test', "DictWriter Test", keys=['date', 'x', 'y'])
        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...         {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
        >>> for row in rows:
        ...     writer.writerow(row)
        ... 
        >>> writer.writesource()
        >>> writer.flush()
        >>> hash(open('./datasources/dictwriter_test.yaml').read())
        2318934412299633258
        >>> hash(open('./datafiles/dictwriter_test.csv').read())
        -3310066083987888095
        >>>
    """

    def __init__(self, *args, **kwargs):
        kwargs['date_key'] = 'date'
        if 'keys' not in kwargs:
            kwargs['keys'] = None
        super(DictWriter, self).__init__(*args, **kwargs)


    def init_keys(self):
        if not self.writer:
            csv_path = os.path.join(self.datafile_dir, self.csv_name)
            self.csv_file = open(csv_path, 'w')
            self.writer = csv.DictWriter(self.csv_file, self.keys, restval='', extrasaction='ignore')
            self.writer.writeheader()


    def init_from_row(self, row):
        logging.debug('inferring keys from first row')
        self.keys = sorted(row.keys())
        self.keys.remove(self.date_key)
        self.keys.insert(0,self.date_key)
        self.init_keys()


    def writerow(self, row):
        # dict writer differs from writer in that it requires the keys before
        # it can write a row, so it figures out the keys on the first row
        if not self.writer:
            self.init_from_row(row)
        super(DictWriter, self).writerow(row)



