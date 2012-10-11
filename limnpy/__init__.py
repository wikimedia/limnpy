import csv, yaml
import os, logging
import datetime
from operator import itemgetter


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

        >>> import limnpy, datetime
        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...         {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
        >>> writer = limnpy.DictWriter('evan_test', "Evan's Test", rows)
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
                 rows,
                 date_key='date',
                 keys=None,
                 basedir='.',
                 types=None):
        self.id = id
        self.name = name
        self.rows = rows
        self.date_key = date_key
        self.start = None
        self.end = None

        # deal with defaults

        if keys is None:
            self.keys = sorted(reduce(set.__ior__, map(set,map(dict.keys, self.rows)), set()))
            self.keys.remove(self.date_key)
            self.keys.insert(0,self.date_key)
        logging.info('using keys: %s', self.keys)

        self.rows = sorted(rows, key=itemgetter(self.date_key))
        self.start = rows[0][self.date_key]
        self.end = rows[-1][self.date_key]

        if types:
            if isinstance(types[0], str):
                self.types = types
            else:
                self.types = map(DictWriter.type_map.get, types)
        else:
            self.types = ['date'] + ['int']*(len(self.keys) - 1)
        assert self.types[0] == 'date'
            
        self.csv_name = '%s.csv' % self.id
        self.yaml_name = '%s.yaml' % self.id
        
        # check that output directories exist
        self.datafile_dir = os.path.join(basedir, 'datafiles')
        if not os.path.isdir(self.datafile_dir):
            os.mkdir(self.datafile_dir)
        self.datasource_dir = os.path.join(basedir, 'datasources')
        if not os.path.isdir(self.datasource_dir):
            os.mkdir(self.datasource_dir)

        # create dictwriter to write datafile
        csv_path = os.path.join(self.datafile_dir, self.csv_name)
        self.csv_file = open(csv_path, 'w')
        self.writer = csv.DictWriter(self.csv_file, self.keys, restval='', extrasaction='ignore')
        self.__write_rows__()
        self.__write_datasource__()


    def __write_rows__(self):
        for row in self.rows:
            self.__write_row__(row)
        self.csv_file.close()


    def __write_row__(self, row):
        date_key = self.keys[0]
        if self.start == None or row[date_key] < self.start:
            self.start = row[date_key]
        if self.end == None or row[date_key] > self.end:
            self.end = row[date_key]
        if not isinstance(row[date_key], str):
            row[date_key] = row[date_key].strftime(DictWriter.date_fmt)
        self.writer.writerow(row)


    def __write_datasource__(self):
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

