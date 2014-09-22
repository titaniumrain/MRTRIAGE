'''

THIS IS PROTOTYPE EXTRACTION DEMO

'''




from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

from sets import Set

from GRAPHProtocol import GRAPHProtocol


import get_dist_func

import ast

class MRJobPrototypeExtraction(MRJob):


    INPUT_PROTOCOL = GRAPHProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def configure_options(self):
            super(MRJobPrototypeExtraction, self).configure_options()
            
            self.add_passthrough_option(
                '--feature', default='subject')

            self.add_passthrough_option(
                '--sim-threshold', default='0.9')
    
    
    # can be replaced by parameters read from config file in reducer_init


    
    def mapper(self, key, value):
        '''
        
        Mapper input: 
        
        integer label, oid, longitude, latitude, <optional nodes attached to it, comma separared tuples>
        
        e.g.
        
        Example 1. Node with out nodes attached
        
        1 4ebbd37d466e8b0b55000000 -100.2 32.9
        
        Example 2. Node with nodes attached
        
        2 4ebbd37d466e8b0e55000000 20.1 138.1 ('4ebbd37d466e8b0b55000000', value_of_similarity)
        
        ...
        
        
        Mapper output: 
        
        integer label, [(oid, longitude, latitude), (oid, longitude, latitude), ...]
        
        e.g.
        
        1 [(4ebbd37d466e8b0b55000000 -100.2 32.9), (4ebbd37d466e8b0b5500000a -30.2 50.7), ... ]
        
        ...
        
        
        '''

        yield key, ast.literal_eval(value)
        
        
    def reducer_init(self):
        self.feature = self.options.feature
        self.threshold = float(self.options.sim_threshold)

    
    
    def reducer(self, key, values):
        
        '''
        
        Re-assemble the matrix and do the calculation
        
        Reducer intput: 
        
        integer label, [(oid, longitude, latitude, <optional attached nodes>), (oid, longitude, latitude, <optional attached nodes>), ...]
        
        Reducer output: 
        
        oid, longitude, latitude, attached nodes
        
        Given an example input file as
        
        
        1       a       10      20      ('f',20)
        1       b       10      19
        1       c       10      18
        1       d       10      17
        
        
        The mapper output, in the other words, reducer input looks like
        
        
        1, "a       10      20      ('f',20)", "b       10      19",  "c       10      18", "d       10      17"
        
        This forms a matrix as
          a b c d
        a
        b
        c
        d
        
        If the threshold hold of similarity is set to be 10
        
        
        The reducer output is 
        
        a	10	20	(f,20),('b', 1.0),('c', 2.0),('d', 3.0)
        
        
        It means 'a' is the selected prototype, and 'f,b,c,d' are nodes attached to it with similarity value/distance shown as the second value in the tuple.
        
        
        '''
        
        
        
        pruned = Set()
        nodes = []

        # medium memory operation
        for item in values:
            nodes.append(item)

        # heavy memory operation due to nested loops
        sim_func = get_dist_func.get_dist_func(self.feature)
        for i in range(0, len(nodes)):
            if i not in pruned:
                flag = False
                neighbours = ""
                for j in range(i+1, len(nodes)):
                    if j not in pruned:
                        if self.feature in nodes[i] and self.feature in nodes[j]:
                            dist = float(sim_func(nodes[i][self.feature],
                                                  nodes[j][self.feature]))
                        else:
                            dist = 0

                        if dist > self.threshold:
                            flag = True
                            pruned.add(j)
                            if len(neighbours) == 0:
                                neighbours = str((nodes[j]['key'], dist))
                            else:
                                neighbours = neighbours + " " + str((nodes[j]['key'], dist))
                            if 'neighbours' in nodes[j]:
                                neighbours = neighbours + " " + nodes[j]['neighbours']
                if flag:
                    pruned.add(i)

                    if 'neighbours' in nodes[i]:
                        nodes[i]['neighbours'] = nodes[i]['neighbours'] + " " + neighbours
                    else:
                        nodes[i]['neighbours'] = neighbours

                    yield None, nodes[i]
                elif 'neighbours' in nodes[i]:
                    yield None, nodes[i]
                elif len(nodes) == 1 and 'neighbours' in nodes[0]:
                    yield None, nodes[0]



    

if __name__ == '__main__':    
    MRJobPrototypeExtraction.run()
