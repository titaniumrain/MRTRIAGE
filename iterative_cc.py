from mrjob.job import MRJob
from GRAPHProtocol import GRAPHProtocol



'''

The key of the iterative algorithm is passing on a message to the rest of the nodes that connect to the same component


"In the component I connect to, the minimum node_id is "


For example, given


1	2,3	1
2	1,3	1
3	1,2	1
4	6	4
6	4,7	4
7	8,6	6
8	7	7

First iteration outputs


1	3,2	1
2	1,3	1
3	1,2	1
4	6	4
6	4,7	4
7	8,6	4  <-------- notice the change here, it means 6 pass on the minimum id (4) to node 7
8	7	6

Second iternation outputs

1	3,2	1
2	1,3	1
3	1,2	1
4	6	4
6	4,7	4
7	8,6	4
8	7	4  <-------- notice the change here, it means 7 pass on the minimum id (4) to node 8


Third iteration outputs

1	3,2	1
2	1,3	1
3	1,2	1
4	6	4
6	4,7	4
7	8,6	4
8	7	4  <-------- notice that nothing changed. The algo converges.

It means the two components are found and the algo converges.

PLEASE NOTE THAT THIS IS FOR ILLUSTRATION PURPOSE ONLY. THE REAL MAPREDUCE MAY NOT SHOW THE SAME BEHAVIOUR.

'''


class MRFindCC(MRJob):
    
    
    INPUT_PROTOCOL = GRAPHProtocol
    OUTPUT_PROTOCOL = GRAPHProtocol


    SORT_VALUES = True


    def mapper(self, key, value):
        

        second_part = value.split('\t', 2)
        neighbours = second_part[0].split(',')

        minimum = second_part[1]
        # pass on the graph structure information I know...
        yield key, ('E', second_part[0], minimum)
        for neighbour in neighbours:
            info = neighbour.split('-')
            item = info[0]
            if item != minimum and item > key:
                # let all the nodes i have info about know that from my perspective the minimum node_id is
                yield item, ('V', minimum, minimum)
        

    def reducer(self, key, values):
        label, edges, minimum = next(values)
        assert label=='E'
        

        original_min = minimum
        
        for label, something, somevalue in values:
            if minimum > somevalue:
                # if minimum node_id changed, we record it. This will be used as the only CONVERGE criteria.
                minimum = somevalue
       
        if original_min > minimum:
            self.increment_counter('cc_mr', 'counter', 1)
 
        # update myself for next round of mapping
        yield str(key), edges + "\t" + str(minimum)

    
    def steps(self):
           return ([self.mr(mapper=self.mapper, reducer=self.reducer)])








if __name__ == '__main__':
    MRFindCC.run()
