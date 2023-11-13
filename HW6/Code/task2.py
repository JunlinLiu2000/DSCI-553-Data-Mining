import sys
import time
import random
from sklearn.cluster import KMeans
import numpy as np

def get_data(input_path):
    x_data = []
    y_data = []
    with open(input_path, 'r') as f:
        for line in f:
            list1 = list(line.strip('\n').split(','))
            x_data.append((list1[0], list1[2:]))
            y_data.append((list1[0], list1[1]))
    return x_data, y_data

def remove_outliers(labels, step1_data):
    dic = {}
    for i in range(len(labels)):
        if labels[i] in dic:
            dic[labels[i]].append(i)
        else:
            dic[labels[i]] = [i]

    remove_list = []
    for key, value in dic.items():
        if len(value) == 1:
            remove_list.append((key, value))

    for i in remove_list:
        step1_data.pop(i[1][0])

    return step1_data, remove_list

def kmeans(n_cluster, data):
    kmeans = KMeans(n_clusters = n_cluster, random_state=1).fit(data)
    labels = kmeans.labels_

    return labels

def sum_function(list1, list2):
    sum_list = []
    for j in range(len(list1)):
        sum = float(list1[j]) + float(list2[j])
        sum_list.append(sum)
    return sum_list
def sq_function(list1):
    list2 = []
    for i in list1:
        list2.append(float(i)**2)
    return list2

def make_dic(labels, step4_data):
    DS_dic = {}
    for i in range(len(labels)):
        if labels[i] in DS_dic:
            DS_dic[labels[i]]['N'].append(step4_data[i])
            DS_dic[labels[i]]['SUM'] = sum_function(DS_dic[labels[i]]['SUM'], step4_data[i])
            DS_dic[labels[i]]['SUMSQ'] = sum_function(DS_dic[labels[i]]['SUMSQ'], sq_function(step4_data[i]))
        else:
            DS_dic[labels[i]] = {}
            DS_dic[labels[i]]['N'] = [step4_data[i]]
            DS_dic[labels[i]]['SUM'] = step4_data[i]
            DS_dic[labels[i]]['SUMSQ'] = sq_function(step4_data[i])
    return DS_dic

if __name__ == '__main__':
    start_time = time.time()
    input_path = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file_path = sys.argv[3]

    #step 1
    x_data, y_data = get_data(input_path)
    random.shuffle(x_data)
    input_data = []
    for i in x_data:
        input_data.append(i[1])
    print(input_data[:3])
    length = len(x_data)
    step1_data = input_data[0: int(length/5)]

    #step 2
    large_cluster = n_cluster * 5
    labels = kmeans(large_cluster, step1_data)

    #step 3
    step4_data, RS = remove_outliers(labels, step1_data)

    #step 4
    labels = kmeans(n_cluster, step4_data)

    #step 5
    DS_dic = {}
    for i in range(len(labels)):
        if labels[i] in DS_dic:
            DS_dic[labels[i]]['N'].append(step4_data[i])
            DS_dic[labels[i]]['SUM'] = sum_function(DS_dic[labels[i]]['SUM'], step4_data[i])
            DS_dic[labels[i]]['SUMSQ'] = sum_function(DS_dic[labels[i]]['SUMSQ'], sq_function(step4_data[i]))
        else:
            DS_dic[labels[i]] = {}
            DS_dic[labels[i]]['N'] = [step4_data[i]]
            DS_dic[labels[i]]['SUM'] = step4_data[i]
            DS_dic[labels[i]]['SUMSQ'] = sq_function(step4_data[i])

    #step 6
    print(DS_dic)

