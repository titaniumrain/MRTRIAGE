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

    VALIDILITY = sys.argv[5]

    user_name = sys.argv[6]

    if VALIDILITY == 'only':
        try:
            cmd = './validility'
            os.chdir(cmd)

            for feature in features:
                cmd = 'python PE_analysis.py' + ' ' + feature + ' ' + window_size
                os.system(cmd)

            for feature in features:
                cmd = 'python CC_analysis.py' + ' ' + feature + ' ' + window_size
                os.system(cmd)

            cmd = '..'
            os.chdir(cmd)
            sys.exit(0)
        except Exception as e:
            print e.message





    for feature in features:

        start = datetime.datetime.now()

        cmd = 'python single_feature_prototypes_extraction_driver.py' + ' ' + dataset_name + ' ' + \
                    feature + ' ' + window_size + ' ' + matrix_block_size + ' ' + user_name
        os.system(cmd)

        end = datetime.datetime.now()

        with open("performance.txt", "a") as myfile:
            line = dataset_name + ' ' + 'PE' + ' ' + feature + ' ' + window_size + ' ' + str((end - start).seconds) + '\n'
            myfile.write(line)


        start = datetime.datetime.now()

        cmd = 'python single_feature_pairwisematrix_driver.py' + ' ' + feature + ' ' + matrix_block_size + ' ' + dataset_name + ' ' + window_size + ' ' + user_name
        os.system(cmd)


        cmd = 'python CCDriver.py' + ' ' + feature + ' ' + dataset_name + ' ' + window_size + ' ' + user_name
        os.system(cmd)
        end = datetime.datetime.now()

        with open("performance.txt", "a") as myfile:
            line = dataset_name + ' ' + 'CC' + ' ' + feature + ' ' + window_size + ' ' + str((end - start).seconds) + '\n'
            myfile.write(line)



    if VALIDILITY == 'true':
        cmd = './validility'
        os.chdir(cmd)

        for feature in features:
            cmd = 'python PE_analysis.py' + ' ' + feature + ' ' + window_size
            os.system(cmd)

        for feature in features:
            cmd = 'python CC_analysis.py' + ' ' + feature + ' ' + window_size
            os.system(cmd)


        cmd = '..'
        os.chdir(cmd)

