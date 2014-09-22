from mrjob.job import MRJob
from GRAPHProtocol import GRAPHProtocol
from mrjob.protocol import RawProtocol
from collections import defaultdict


import get_dist_func
import ast




class MRJobPairwiseMatrix(MRJob):
    
    INPUT_PROTOCOL = GRAPHProtocol


    
    
    def configure_options(self):
        super(MRJobPairwiseMatrix, self).configure_options()
        self.add_passthrough_option(
            '--num_of_slices', default='1')
        self.add_passthrough_option(
            '--feature', default='subject')
        self.add_passthrough_option(
            '--sim-threshold', default='0.6')
   
    
    def mapper_init(self):
        self.stripes = defaultdict(list)
        self.num_of_slices = int(self.options.num_of_slices)
        
        
    
    def mapper(self, key, value):
        split_point = int(key)
        for i in range(1, split_point):
            composite_key = key + "_" + str(i)
            self.stripes[composite_key].append((key, ast.literal_eval(value)))
        for j in range(split_point, self.num_of_slices):
            composite_key = str(j) + "_" + key
            self.stripes[composite_key].append((key, ast.literal_eval(value)))
    
    def mapper_final(self):
        for key, val in self.stripes.iteritems():
            yield key, val
    
    
    def combiner_init(self):
        self.combine_stripes=defaultdict(list)
    
    
    def combnier(self, key, value):
        for item in value:
            self.combine_stripes[key] += item

    def combiner_final(self):
        for key, val in self.combine_stripes.iteritems():
            yield key, val
    
    def reducer_init(self):
        self.feature = self.options.feature
        self.threshold = float(self.options.sim_threshold)
    
    
    def reducer(self, key, value_list):
        data = defaultdict(list)
        keys = key.split('_')
        x = keys[0]
        y = keys[1]


        if (x == y):
            items = []
            for item in value_list:
                for unit in item:
                    items.append(unit[1])
            self.calculator_1(items)
        else:
            for item in value_list:
                for unit in item:
                    data[unit[0]].append(unit[1])
            self.calculator_2(x, y, data)


    # nodes param is list type
    def calculator_1(self, nodes):
        sim_func = get_dist_func.get_dist_func(self.feature)
        for i in range(0, len(nodes) - 1):
            for j in range(i+1, len(nodes)):
                if self.feature in nodes[i] and self.feature in nodes[j]:
                    dist = sim_func(nodes[i][self.feature], nodes[j][self.feature])
                else:
                    dist = 0.0
                if dist > self.threshold:
                    print nodes[i]['key'] + "\t" + nodes[j]['key'] + "\t" + str(dist)
    
    
    # nodes param is dict type
    def calculator_2(self, x, y, nodes):
        sim_func = get_dist_func.get_dist_func(self.feature)
        for i in range(0, len(nodes[x])):
            for j in range(0, len(nodes[y])):
                if self.feature in nodes[x][i] and self.feature in nodes[y][j]:
                    dist = sim_func(nodes[x][i][self.feature], nodes[y][j][self.feature])
                else:
                    dist = 0.0
                if dist > self.threshold:
                    print nodes[x][i]['key'] + "\t" + nodes[y][j]['key'] + "\t" + str(dist)

                   
if __name__ == '__main__':
    MRJobPairwiseMatrix.run()