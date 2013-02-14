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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Dashboard(object):

    default_dashboard = {
        'name' : '',
        'headline' : '',
        'subhead'  : '',
        'tabs': []}

    def __init__(self, id, name, headline = '', subhead='', tabs=None):
        self.id = id
        self.__dashboard__ = copy.deepcopy(Dashboard.default_dashboard)
        self.__dashboard__['name'] = name
        self.__dashboard__['headline'] = headline
        self.__dashboard__['subhead'] = subhead
        self.__dashboard__['tabs'] = tabs if tabs is not None else []

    def add_tab(self, name, graph_ids=[]):
        tab =  {  
            "name" : name,
            "graph_ids" : graph_ids}
        self.__dashboard__['tabs'].append(tab)


    def write(self, basedir='.'):
        db_dir = os.path.join(basedir, 'dashboards')
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        db_path = os.path.join(db_dir, self.id + '.json')
        json.dump(self.__dashboard__, open(db_path, 'w'), indent=2)

    def __str__(self):
        return json.dumps(self.__dashboard__, indent=2)
