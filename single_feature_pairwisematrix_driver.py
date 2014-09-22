from subprocess import call


import sys
import subprocess
import math

hadoop_bin = subprocess.Popen(["which","hadoop"],stdout=subprocess.PIPE).communicate()[0]
hadoop_bin = hadoop_bin.replace('\n', '')


if __name__ == '__main__':


    feature_name = sys.argv[1]
    block_size = sys.argv[2]


    dataset_name = sys.argv[3]
    window_size = sys.argv[4]


    user_name = sys.argv[5]


    HD_BASE = 'hdfs:///user/' + user_name + '/data/' + dataset_name + "_" + window_size + "_"




    counter = 0
    proc = subprocess.Popen(['hadoop', 'fs', '-cat', HD_BASE + feature_name + '6' + '/*'], stdout=subprocess.PIPE)
    while True:
        line = proc.stdout.readline()
        if line != '':
            counter += 1
        else:
            break


    slices = math.ceil(counter/float(block_size))



    return_code = call(['hadoop', 'fs', '-rmr', HD_BASE + feature_name + '_pairwisematrix'])


    return_code = call(['python', 'single_feature_pairwisematrix.py', '--num_of_slices', str(int(slices)), '--no-output', '-r',
                        'hadoop', HD_BASE + feature_name + '6', '--output',
                        HD_BASE + feature_name + '_pairwisematrix', '--feature', feature_name,
                        '--python-archive', 'protocols.tar.gz', '--python-archive', 'sim_funcs.tar.gz', '--hadoop-bin', hadoop_bin])
