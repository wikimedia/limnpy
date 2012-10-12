import csv, yaml
import os, logging
import datetime
from operator import itemgetter



def write(id, name, rows, **kwargs):
    """
    Intsance level convience method for carrying out all of the steps
    necessary to write a datafile and datasource

        >>> import limnpy, datetime
        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...         {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
        >>> limnpy.write('evan_test', "Evan's Test", rows)
        >>> print open('./datasources/evan_test.yaml').read().strip()
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
        id: evan_test
        name: Evan's Test
        shortName: Evan's Test
        timespan:
          end: 2012-10-01
          start: 2012-09-01
          step: 1d
        url: ../datafiles/evan_test.csv
        >>> print open('./datafiles/evan_test.csv').read().strip() # doctest: +NORMALIZE_WHITESPACE
        2012/09/01,1,2
        2012/10/01,7,9
        >>>
    """
    dwriter = DictWriter(id, name, **kwargs)
    dwriter.write_rows(rows)
    dwriter.write_datasource()


class DictWriter(object):
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

    here's a simple example (set up for doctest)


        >>> import limnpy, datetime, pprint
        >>> writer = limnpy.DictWriter('evan_test', "Evan's Test", keys=['date', 'x', 'y'])
        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...         {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
        >>> writer.write_rows(rows)
        >>> print open('./datasources/evan_test.yaml').read().strip()
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
        id: evan_test
        name: Evan's Test
        shortName: Evan's Test
        timespan:
          end: 2012-10-01
          start: 2012-09-01
          step: 1d
        url: ../datafiles/evan_test.csv
        >>> print open('./datafiles/evan_test.csv').read().strip() # doctest: +NORMALIZE_WHITESPACE
        2012/09/01,1,2
        2012/10/01,7,9
        >>>
    """


    type_map = {int : 'int',
                float : 'int',
                datetime.datetime : 'date',
                datetime.date : 'date'}

    date_fmt = '%Y/%m/%d'

    def __init__(self, 
                 id, 
                 name,
                 date_key='date',
                 keys=None,
                 basedir='.',
                 types=None):
        self.id = id
        self.name = name
        self.keys = keys
        self.date_key = date_key

        # will be populate by actually writing rows
        self.start = None
        self.end = None
        # will be populated by init_from_keys
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
            self.init_from_keys()



    def init_from_keys(self):
        if not self.writer:
            csv_path = os.path.join(self.datafile_dir, self.csv_name)
            self.csv_file = open(csv_path, 'w')
            self.writer = csv.DictWriter(self.csv_file, self.keys, restval='', extrasaction='ignore')


    def init_from_row(self, row):
        logging.info('inferring keys from first row')
        self.keys = sorted(row.keys())
        self.keys.remove(self.date_key)
        self.keys.insert(0,self.date_key)
        self.init_from_keys()


    def write_row(self, row):
        if not self.writer:
            self.init_from_row(row)

        if self.start is None or row[self.date_key] < self.start:
            self.start = row[self.date_key]
        if self.end is None or row[self.date_key] > self.end:
            self.end = row[self.date_key]
        if not isinstance(row[self.date_key], basestring):
            row[self.date_key] = row[self.date_key].strftime(DictWriter.date_fmt)

        self.writer.writerow(row)


    def write_rows(self, rows):
        rows = sorted(rows, key=itemgetter(self.date_key))
        self.init_from_row(rows[0])
        self.write_header()
        for row in rows:
            self.write_row(row)
        self.csv_file.close()


    def write_header(self):
        assert self.writer, 'writer has not been initialized. cannot write header row because don\'t know the keys'
        self.writer.write_header()


    def write_datasource(self):
        assert self.writer, 'no rows have been written. cannot write datasource'

        self.types = ['date'] + ['int']*(len(self.keys) - 1)

        meta = {}
        meta['id'] = self.id
        meta['name'] = self.name
        meta['shortName'] = meta['name']
        meta['format'] = 'csv'
        meta['url'] = '../datafiles/' + self.csv_name

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

