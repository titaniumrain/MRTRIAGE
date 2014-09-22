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






class MRFilterPrototypes(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol


    SORT_VALUES = True


    def mapper(self, _, line):

        if line.get('prototype_id') is not None:
            yield str(line['prototype_id']), ('*', 1)
        else:
            yield str(line['key']), ('V', line)


    def reducer(self, key, values):
        marker, item = next(values)
        if marker == '*':
            marker, value = next(values)
            yield None, value




if __name__ == '__main__':
    MRFilterPrototypes.run()
