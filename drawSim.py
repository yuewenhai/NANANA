import pandas as pd
from matplotlib import pyplot as plt

top = 'StringResult'
block_method = 'Soundex'
methods = ['Normal', 'ULDP', 'RESAM', 'WXOR']
attributes = [['PERNAME1', 'PERNAME2', 'SEX'],
              ['PERNAME1', 'PERNAME2', 'SEX', 'DOB_YEAR'],
              ['PERNAME1', 'PERNAME2', 'SEX', 'DOB_YEAR', 'PERSON_ID']]
epsilon = 1.0

data_size = 20000
bit_vector_size = 1000
n_grams = 2
k = 30
datasets_name = 'census-cis-'
dataset_name = 'Istat'


def array2str(array):
    string = ''
    for item in array:
        string += item + '-'
    return string


for method in methods:
    for attribute in attributes:
        sim = []
        es_sim = []
        for i in range(8):
            file_path = top + '/' + datasets_name + '/' + method + '/' + block_method + '/' + array2str(attribute)
            file_name = dataset_name + '-' + str(data_size) + '-' + str(bit_vector_size) + '-' + str(
                n_grams) + '-' + str(k) + '-' + str(epsilon) + '-' + str(i) + '.txt'
            temp_result = pd.read_table(file_path + '/' + file_name, sep=' ')
            sim.append(temp_result[:][0])
            es_sim.append(temp_result[:][1])

        plt.scatter(sim, es_sim)
        plt.show()
