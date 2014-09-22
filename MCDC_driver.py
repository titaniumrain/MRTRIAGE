import os
import sys

import datetime
import subprocess



hadoop_bin = subprocess.Popen(["which","hadoop"],stdout=subprocess.PIPE).communicate()[0]
hadoop_bin = hadoop_bin.replace('\n', '')


if __name__ == '__main__':

    dataset_name = sys.argv[1]
    features = sys.argv[2].split(',')
    window_size = sys.argv[3]
    matrix_block_size = sys.argv[4]
    steps = sys.argv[5]

    VALIDILITY = sys.argv[6]

    user_name = sys.argv[7]



    HD_BASE = 'hdfs:///user/' + user_name + '/data/' + dataset_name + "_" + window_size + "_"

    if steps == '1' or steps == '3':

        for feature in features:

            cmd = 'python single_feature_clustering_driver.py' + ' ' + dataset_name + ' ' + \
                    feature + ' ' + window_size + ' ' + matrix_block_size + ' ' + VALIDILITY + ' ' + user_name

            os.system(cmd)




    if steps == '2' or steps == '3':
        # Code removed due to IP protection