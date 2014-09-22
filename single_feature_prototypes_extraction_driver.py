from subprocess import call


import sys
import subprocess


import time





hadoop_bin = subprocess.Popen(["which","hadoop"],stdout=subprocess.PIPE).communicate()[0]
hadoop_bin = hadoop_bin.replace('\n', '')



if __name__ == '__main__':

    dataset_name = sys.argv[1]
    feature_name = sys.argv[2]
    block_size = sys.argv[3]

    # fixed value for matrix computation
    matrix_slice = sys.argv[4]

    user_name = sys.argv[5]


    HD_BASE = 'hdfs:///user/' + user_name + '/data/' + dataset_name + "_" + block_size + "_"


    extracted_data = 'extracted_data'


    # Step 1: Extract data
    return_code = call(['hadoop', 'fs', '-rmr', HD_BASE + extracted_data])
    return_code = call(['python', 'extract_features.py', '--no-output', 'hdfs:///user/' + user_name + '/data/' + dataset_name,
                        '--output', HD_BASE + extracted_data, '-r', 'hadoop', '--features', feature_name, '--hadoop-bin', hadoop_bin])



    return_code = call(['hadoop', 'fs', '-rmr', HD_BASE + feature_name + '0'])
    return_code = call(['hadoop', 'jar', 'linenum.jar', 'RowNumberJob', HD_BASE + extracted_data,
                        HD_BASE + feature_name + '0', '80', block_size])



    for i in [0, 2, 4]:

        j = i + 1
        intermediate_data = HD_BASE + feature_name + str(j) + '_with_NB'
        return_code = call(['hadoop', 'fs', '-rmr', intermediate_data])

        return_code = call(['python', 'single_feature_prototypes_extraction.py', '--no-output',
                            HD_BASE + feature_name + str(i), '--output',
                            intermediate_data, '-r', 'hadoop', '--feature', feature_name,
                            '--python-archive=sim_funcs.tar.gz', '--python-archive=protocols.tar.gz', '--hadoop-bin', hadoop_bin])


        clean_data = HD_BASE + feature_name + str(j)
        return_code = call(['hadoop', 'fs', '-rmr', clean_data])


        return_code = call(['python', 'single_feature_split_out_neighbours.py', '--no-output',
                            intermediate_data, '--output',
                            clean_data, '-r', 'hadoop', '--feature', feature_name, '--hadoop-bin', hadoop_bin])


        k = j + 1
        intermediate_data = HD_BASE + feature_name + str(k)
        return_code = call(['hadoop', 'fs', '-rmr', intermediate_data])



        if i == 4:
            return_code = call(['hadoop', 'jar', 'linenum.jar', 'RowNumberJob', HD_BASE + feature_name + str(j),
                        HD_BASE + feature_name + str(k), '80', matrix_slice])

            sys.exit(0)


        else:
            return_code = call(['hadoop', 'jar', 'linenum.jar', 'RowNumberJob', HD_BASE + feature_name + str(j),
                        HD_BASE + feature_name + str(k), '80', block_size])

