from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


class MRJobPESplitOutNeighbours(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    SORT_VALUES = True

    def configure_options(self):
        super(MRJobPESplitOutNeighbours, self).configure_options()

        self.add_passthrough_option(
            '--feature', default='subject')

    def mapper_init(self):
        self.feature = self.options.feature


    def mapper(self, _, line):
        values = {}
        values['key'] = line['key']
        values[self.feature] = line[self.feature]
        values['neighbours'] = ""
        yield None, values


if __name__ == '__main__':
    MRJobPESplitOutNeighbours.run()