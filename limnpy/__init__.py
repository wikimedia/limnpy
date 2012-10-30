import csv, yaml, json
import os, logging
import datetime
from operator import itemgetter
from collections import Sequence, MutableSequence
import codecs
import colorbrewer
import itertools

logger = logging.getLogger(__name__)

limn_date_fmt = '%Y/%m/%d'

def write(id, name, keys, rows, **kwargs):
    """
    Intsance level convience method for carrying out all of the steps
    necessary to write a datafile and datasource

        >>> import limnpy, datetime
        >>> rows = [[datetime.date(2012, 9, 1), 1, 2],
        ...         [datetime.date(2012, 10, 1), 7, 9]]
        >>> source = limnpy.write('write_test', 'Write Test', ['date', 'x', 'y'], rows)
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
    if len(rows) == 0:
        logger.warning('no datafile or datasource created because rows is empty')
        return None
    writer = Writer(id, name, keys, **kwargs)
    writer.writerows(rows)
    return writer.writesource()


def writedicts(id, name, rows, **kwargs):
    """
    Intsance level convience method for carrying out all of the steps
    necessary to write a datafile and datasource

        >>> import limnpy, datetime
        >>> rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        ...         {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
        >>> source = limnpy.writedicts('writedicts_test', "Write Dicts Test", rows)
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
    if not rows:
        logger.warning('no datafile or datasource created because rows is empty')
        return None
    dwriter = DictWriter(id, name, **kwargs)
    dwriter.writerows(rows)
    return dwriter.writesource()


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

        # will be populated by actually writing rows
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
            self.csv_file = codecs.open(csv_path, encoding='utf-8', mode='w')
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
            if isinstance(row, Sequence) and not isinstance(row, MutableSequence):
                row = list(row)
            row[self.date_key] = row[self.date_key].strftime(limn_date_fmt)
        self.writer.writerow(row)

    def flush(self):
        if hasattr(self, 'csv_file'):
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
        return meta


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
        >>> s = writer.writesource()
        >>> writer.flush()
        >>> hash(open('./datasources/dictwriter_test.yaml').read())
        2318934412299633258
        >>> hash(open('./datafiles/dictwriter_test.csv').read())
        -3310066083987888095
        >>>
    """

    def __init__(self, *args, **kwargs):
        if 'keys' not in kwargs:
            kwargs['keys'] = None
        super(DictWriter, self).__init__(*args, **kwargs)
        if 'date_key' not in kwargs:
            self.date_key = 'date'


    def init_keys(self):
        if not self.writer:
            csv_path = os.path.join(self.datafile_dir, self.csv_name)
            self.csv_file = codecs.open(csv_path, encoding='utf-8', mode='w')
            self.writer = csv.DictWriter(self.csv_file, self.keys, restval='', extrasaction='ignore')
            self.writer.writeheader()


    def init_from_row(self, row):
        logger.debug('inferring keys from first row')
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


def metric(source, index, col_key, color=None):
    if isinstance(col_key, basestring):
        col_key = source['columns']['labels'].index(col_key)
    assert isinstance(col_key, int), 'col_key must either be a column index as an int or column label as a str'
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
        "label": "",
        "disabled": False,
        "visible": True,
        "format_value": None,
        "transforms": [],
        "source_id": source['id'],
        "chartType": None,
        "type": "int",
        "source_col": col_key
        }
    return metric

def get_color_map(n):
    # get colorspace based on number of metrics
    family = colorbrewer.Spectral
    if n < 3:
        color_map = family[3][:n]
    elif n > 11:
        logger.warning('too many metrics, looping over color space')
        color_map = itertools.cycle(family[11])
    else:
        color_map = family[n]
    return list(itertools.islice(color_map, None, n))

def writegraph(slug, name, sources, metric_ids=None, basedir='.', meta={}, options={}):
    """
    Creates limn compatible graph file with the provided options.

        >>> import limnpy, datetime
        >>> rows1 = [[datetime.date(2012, 9, 1), 1, 2],
        ...         [datetime.date(2012, 10, 1), 7, 9]]
        >>> s1 = limnpy.write('source1', 'Source 1', ['date', 'x', 'y'], rows1)
        >>> rows2 = [[datetime.date(2012, 9, 1), 19, 22],
        ...         [datetime.date(2012, 10, 1), 27, 29]]
        >>> s2 = limnpy.write('source2', 'Source 2', ['date', 'x', 'y'], rows2)
        >>> limnpy.writegraph('my_first_autograph', 'My First Autograph', [s1, s2], [('source1', 'x'), ('source2', 'y')])
        >>> hash(open('./graphs/my_first_autograph.json').read())
        -6544027546604027079

    or just pass in the sources and a graph will be constructed containing all of the columns
    in all of the sources

        >>> import limnpy, datetime
        >>> rows1 = [[datetime.date(2012, 9, 1), 1, 2],
        ...         [datetime.date(2012, 10, 1), 7, 9]]
        >>> s1 = limnpy.write('source1', 'Source 1', ['date', 'x', 'y'], rows1)
        >>> rows2 = [[datetime.date(2012, 9, 1), 19, 22],
        ...         [datetime.date(2012, 10, 1), 27, 29]]
        >>> s2 = limnpy.write('source2', 'Source 2', ['date', 'x', 'y'], rows2)
        >>> limnpy.writegraph('my_first_default_autograph', 'My First Default Autograph', [s1, s2])
        >>> hash(open('./graphs/my_first_default_autograph.json').read())
        -9151737922308482552

    """

    graphdir = os.path.join(basedir, 'graphs')
    if not os.path.isdir(graphdir):
        os.mkdir(graphdir)
    graph_fn = os.path.join(graphdir, '%s.json' % slug)

    if metric_ids is None:
        metric_ids = []
        for source in sources:
            for col_id in range(1, len(source['columns']['labels'])):
                metric_ids.append((source['id'], col_id))

    color_map = get_color_map(len(metric_ids))

    metrics = []
    source_dict = {source['id'] : source for source in sources}
    for i, (source_id, col_key) in enumerate(metric_ids):
        source = source_dict[source_id]
        try:
            m = metric(source_dict[source_id], i, col_key, color_map[i])
            metrics.append(m)
        except ValueError:
            logger.warning('Could not find column label: %s in datasource: %s', col_key, source['id'])
    
        
    graph = {
        "name": name,
        "notes": "",
        "callout": {
            "enabled": True,
            "metric_idx": 0,
            "label": ""
            },
        "slug": "ar_wp_active",
        "width": "auto",
        "parents": ["root"],
        "result": "ok",
        "id": slug,
        "chartType": "dygraphs",
        "height": 320,
        "data": {
            "metrics": metrics,
            "palette": None,
            "lines": []
            },
        "options": {
            "strokeWidth": 2,
            },
        "desc": ""
        }

    for name, val in meta:
        graphs[name] = val

    for name, val in options:
        graphs['options'][name] = val
    
    json.dump(graph, codecs.open(graph_fn, encoding='utf-8', mode='w'), indent=2)
