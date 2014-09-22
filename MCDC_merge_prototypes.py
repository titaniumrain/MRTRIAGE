import mrjob

from mrjob.job import MRJob
import re
from mrjob.protocol import JSONValueProtocol

import sys

try:
    import simplejson as json
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json

from GRAPHProtocol import GRAPHProtocol

import ast


class MRMergePrototypes(MRJob):

    INPUT_PROTOCOL = GRAPHProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol


    SORT_VALUES = True

    def mapper(self, key, value):
        items = ast.literal_eval(value)
        yield items['key'], 1


    def reducer(self, key, values):
        prototype = {}
        prototype['prototype_id'] = key
        yield None, prototype




if __name__ == '__main__':

    MRMergePrototypes.run()
