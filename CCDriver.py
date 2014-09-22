from iterative_cc import MRFindCC
from subprocess import call

import sys
import subprocess



hadoop_bin = subprocess.Popen(["which","hadoop"],stdout=subprocess.PIPE).communicate()[0]


def driver(inFile, outFile):
    mr_job = MRFindCC(args=['--no-output', '-r', 'hadoop', inFile, '--output', outFile, '--python-archive', 'protocols.tar.gz', '--hadoop-bin', hadoop_bin])
    with mr_job.make_runner() as runner:
        runner.run()
        counters = runner.counters()
        if counters[0] and 'cc_mr' in counters[0]:
            return counters[0]['cc_mr']['counter']
        else:
            return 0
            

if __name__ == "__main__":

    feature_name = sys.argv[1]

    dataset_name = sys.argv[2]
    window_size = sys.argv[3]

    user_name = sys.argv[4]




    HD_BASE = 'hdfs:///user/' + user_name + '/data/' + dataset_name + "_" + window_size + "_"





    seed = HD_BASE + feature_name + '_graph_'
    return_code = call(["hadoop", "fs", "-rmr", seed])

    return_code = call(['python', 'initial_graph_build.py', '-r', 'hadoop',
                        HD_BASE + feature_name + '_pairwisematrix', '--python-archive', 'protocols.tar.gz',
                        '--output', seed, '--no-output', '--hadoop-bin', hadoop_bin])


    flip = 1
    outFile = seed + str(flip)
    
    return_code = call(["hadoop", "fs", "-rmr", outFile])
    
    base = driver(seed, outFile)
    print base
    converge = False

    
    while not converge:
        inFile = seed + str(flip)
        flip += 1
        outFile = seed + str(flip)
        return_code = call(["hadoop", "fs", "-rmr", outFile])
        changes = driver(inFile, outFile)
        return_code = call(["hadoop", "fs", "-rmr", inFile])
        print changes
        if changes == 0:
            converge = True



    final_result = seed + 'final'

    return_code = call(["hadoop", "fs", "-rmr", final_result])

    return_code = call(["hadoop", "fs", "-mv", outFile, final_result])