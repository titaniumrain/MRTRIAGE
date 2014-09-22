from mrjob.job import MRJob
from GRAPHProtocol import GRAPHProtocol

class MRBuildGraph(MRJob):
    
    OUTPUT_PROTOCOL = GRAPHProtocol
    
    
    '''
    
    The key of the mapper to map the minimum node id with the edge
    
    Given an input graph,
    
    1       2
    2       1
    3       2
    2       3
    1       3
    3       1
    4       6
    6       4
    6       7
    7       6
    7       8
    
    
    for a graph
    
    1 - 2
     \ /
      3
    
    4 - 6 - 7 - 8
    
      
      
    The mapper outputs,
    
    
    1	[2, 1]
    2	[1, 1]
    2	[1, 1]
    1	[2, 1]
    3	[2, 2]
    2	[3, 2]
    2	[3, 2]
    3	[2, 2]
    1	[3, 1]
    3	[1, 1]
    3	[1, 1]
    1	[3, 1]
    4	[6, 4]
    6	[4, 4]
    6	[4, 4]
    4	[6, 4]
    6	[7, 6]
    7	[6, 6]
    7	[6, 6]
    6	[7, 6]
    7	[8, 7]
    8	[7, 7]
    
    
    We can see the redundant mapper output, but can simply be resolved by providing one undirected edge. 
    
    The example can be exemplified by checking edge (7,8) above.
    
    
    '''
    
    
    

    def mapper(self, _, line):
        edge = line.split('\t')
        src = edge[0]
        trg = edge[1]

        if len(edge) == 3:
            weight = edge[2]
        else:
            weight = '1.0'

        if src > trg:
            minimum = trg
        else:
            minimum = src
        yield src, (trg, weight, minimum)
        yield trg, (src, weight, minimum)
        


    '''
    
    
    The reducer is to generate the first seed of connected component
    
    
    1	2,3	1
    2	1,3	1
    3	1,2	1
    4	6	4
    6	4,7	4
    7	8,6	6
    8	7	7
    
    
    
    '''
    


    def reducer(self, key, node_list):
        trg, weight, minimum = next(node_list)
        neighbour = set()
        item = trg + '-' + weight
        neighbour.add(item)
        for node, weight, value in node_list:
            item = node + '-' + weight
            neighbour.add(item)
            if minimum > value:
                minimum = value

        yield str(key), ','.join(str(item) for item in neighbour) + "\t" + str(minimum)

if __name__ == '__main__':
    MRBuildGraph.run()
