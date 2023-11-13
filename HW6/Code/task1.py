import sys
import time
import random
from sklearn.cluster import KMeans
import numpy as np
import math
from itertools import combinations

def get_data(input_path):
    x_data = []
    y_data = []
    input_data = []
    with open(input_path, 'r') as f:
        for line in f:
            list1 = list(line.strip('\n').split(','))
            x_data.append((list1[0], list1[2:]))
            y_data.append((list1[0], list1[1]))
            input_data.append(list1[2:])
    return x_data, y_data, input_data

def sum_function(list1, list2):
    sum_list = []
    for j in range(len(list1)):
        sum = float(list1[j]) + float(list2[j])
        sum_list.append(sum)
    return sum_list

def sq_function(list1):
    list2 = []
    for i in list1:
        list2.append(float(i) ** 2)
    return list2

def count_number(DS_dic, CS_dic, RS):
    number_DS = 0
    for key, value in DS_dic.items():
        number_DS = number_DS + len(value['N'])
    number_ccs = len(CS_dic)
    number_CS = 0
    for key, value in CS_dic.items():
        number_CS = number_CS + len(value['N'])
    number_RS = len(RS)
    return number_DS, number_ccs, number_CS,number_RS

def count_distance(point, dic):
    centroid = list(np.array(dic['SUM'])/len(dic['N']))
    std = list((np.array(dic['SUMSQ'])/len(dic['N']) - (np.array(dic['SUM'])/len(dic['N']))**2))
    list1 = []
    for i in range(len(point)):
        a = ((float(point[i])-float(centroid[i]))/float(math.sqrt(std[i])))**2
        list1.append(a)
    distance = math.sqrt(sum(list1))
    return distance

def cluster_distance(dic0, dic1):
    centroid0 = list(np.array(dic0['SUM'])/len(dic0['N']))
    std0 = list((np.array(dic0['SUMSQ'])/len(dic0['N']) - (np.array(dic0['SUM'])/len(dic0['N']))**2))
    centroid1 = list(np.array(dic1['SUM'])/len(dic1['N']))
    std1 = list((np.array(dic1['SUMSQ'])/len(dic1['N']) - (np.array(dic1['SUM'])/len(dic1['N']))**2))
    list1 = []
    list2 = []
    for i in range(len(centroid1)):
        dis1 = ((float(centroid0[i]) - float(centroid1[i])) / float(math.sqrt(std0[i]))) ** 2
        list1.append(dis1)
        dis2 = ((float(centroid0[i]) - float(centroid1[i])) / float(math.sqrt(std1[i]))) ** 2
        list2.append(dis2)
    distance1 = math.sqrt(sum(list1))
    distance2 = math.sqrt(sum(list2))

    return min(distance1, distance2)
def cf(list1):
    list2 = []
    for i in list1:
        list2.append(float(i))
    return list2

def make_answer(DS_dic, CS_dic, RS, x_data):
    last_dic = {}
    for key, value in DS_dic.items():
        for point in value['N']:
            last_dic[tuple(point)] = key
    for key, value in CS_dic.items():
        for point in value['N']:
            last_dic[tuple(point)] = key
    for point in RS:
        last_dic[tuple(point)] = -1

    #print(len(last_dic))

    file_dic = {}
    for i in x_data:
        file_dic[tuple(i[1])] = i[0]

    answer_list = []
    for key, value in file_dic.items():
        answer_list.append((value, last_dic[key]))
    return answer_list

if __name__ == '__main__':
    start_time = time.time()
    input_path = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file_path = sys.argv[3]
    #random.seed(10)
    #step 1
    x_data, y_data, input_data = get_data(input_path)
    random.seed(10)
    random.shuffle(input_data)
    length = len(input_data)
    step1_data = input_data[0: int(length / 5)]

    # step 2
    n_cluster = 10
    large_cluster = n_cluster * 5
    kmeans = KMeans(n_clusters=large_cluster, random_state=1).fit(step1_data)
    labels = kmeans.labels_

    # step 3
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

    RS = []
    for i in remove_list:
        remove_point = step1_data[i[1][0]]
        RS.append(remove_point)
        step1_data.remove(remove_point)

    # step 4
    step4_data = step1_data
    kmeans = KMeans(n_clusters=n_cluster, random_state=1).fit(step4_data)
    labels = kmeans.labels_
    #print(len(labels))

    # step 5
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
    #print(len(DS_dic))

    # step 6
    k = min(int(len(RS) * 0.9), n_cluster)
    kmeans = KMeans(n_clusters=k, random_state=1).fit(RS)
    labels = kmeans.labels_
    # print(labels)
    dic = {}
    for i in range(len(labels)):
        if labels[i] in dic:
            dic[labels[i]].append(i)
        else:
            dic[labels[i]] = [i]
    # print(dic)
    remove_list = []
    cs_list = []
    for key, value in dic.items():
        if len(value) == 1:
            remove_list.append((key, value))
        else:
            cs_list.append((key, value))
    # print(cs_list)

    step6_cs = []
    for i in cs_list:
        for j in i[1]:
            point = RS[j]
            step6_cs.append(point)

    step6_RS = []
    for i in remove_list:
        remove_point = RS[i[1][0]]
        step6_RS.append(remove_point)

    CS_dic = {}
    for i in range(len(labels)):
        if RS[i] not in step6_RS:
            if labels[i] in CS_dic:
                CS_dic[labels[i]]['N'].append(RS[i])
                CS_dic[labels[i]]['SUM'] = sum_function(CS_dic[labels[i]]['SUM'], RS[i])
                CS_dic[labels[i]]['SUMSQ'] = sum_function(CS_dic[labels[i]]['SUMSQ'], sq_function(RS[i]))
            else:
                CS_dic[labels[i]] = {}
                CS_dic[labels[i]]['N'] = [RS[i]]
                CS_dic[labels[i]]['SUM'] = RS[i]
                CS_dic[labels[i]]['SUMSQ'] = sq_function(RS[i])

    RS = step6_RS
    number_DS, number_ccs, number_CS, number_RS = count_number(DS_dic, CS_dic, RS)

    output_file = open(output_file_path, 'w')
    output_file.write("The intermediate results:\n")
    output_file.write(
        "Round 1: " + str(number_DS) + ',' + str(number_ccs) + ',' + str(number_CS) + ',' + str(number_RS) + '\n')

    for round in range(4):
        if round == 3:
            step_data = input_data[int(length / 5) * (round + 1):]
        else:
            step_data = input_data[int(length / 5) * (round + 1):int(length / 5) * (round + 2)]

        d = len(step_data[0])
        for i in range(len(step_data)):
            point = step_data[i]
            distance_list = []
            for key, value in DS_dic.items():
                distance_list.append((count_distance(point, value), key))
            distance_list.sort()
            min_distance = distance_list[0][0]
            if min_distance < 2 * math.sqrt(d):
                DS_dic[distance_list[0][1]]['N'].append(point)
                DS_dic[distance_list[0][1]]['SUM'] = sum_function(DS_dic[distance_list[0][1]]['SUM'], point)
                DS_dic[distance_list[0][1]]['SUMSQ'] = sum_function(DS_dic[distance_list[0][1]]['SUMSQ'],
                                                                    sq_function(point))
            else:
                distance_list = []
                for key, value in CS_dic.items():
                    distance_list.append((count_distance(point, value), key))
                distance_list.sort()
                min_distance = distance_list[0][0]
                if min_distance < 2 * math.sqrt(d):
                    CS_dic[distance_list[0][1]]['N'].append(point)
                    CS_dic[distance_list[0][1]]['SUM'] = sum_function(CS_dic[distance_list[0][1]]['SUM'], point)
                    CS_dic[distance_list[0][1]]['SUMSQ'] = sum_function(CS_dic[distance_list[0][1]]['SUMSQ'],
                                                                        sq_function(point))
                else:
                    RS.append(point)

        k = min(int(len(RS)), n_cluster)
        kmeans = KMeans(n_clusters=k, random_state=1).fit(RS)
        labels = kmeans.labels_

        dic = {}
        for i in range(len(labels)):
            if labels[i] in dic:
                dic[labels[i]].append(i)
            else:
                dic[labels[i]] = [i]

        remove_list = []
        cs_list = []
        for key, value in dic.items():
            if len(value) == 1:
                remove_list.append((key, value))
            else:
                cs_list.append((key, value))

        step11_cs = []
        for i in cs_list:
            for j in i[1]:
                point = RS[j]
                step11_cs.append(point)

        step11_RS = []
        for i in remove_list:
            remove_point = RS[i[1][0]]
            step11_RS.append(remove_point)

        # step11_CS_dic = {}
        for i in range(len(labels)):
            if RS[i] not in step11_RS:
                new_label = labels[i]
                if new_label in CS_dic:
                    CS_dic[new_label]['N'].append(RS[i])
                    CS_dic[new_label]['SUM'] = sum_function(CS_dic[new_label]['SUM'], RS[i])
                    CS_dic[new_label]['SUMSQ'] = sum_function(CS_dic[new_label]['SUMSQ'], sq_function(RS[i]))
                else:
                    CS_dic[new_label] = {}
                    CS_dic[new_label]['N'] = [RS[i]]
                    CS_dic[new_label]['SUM'] = cf(RS[i])
                    CS_dic[new_label]['SUMSQ'] = sq_function(RS[i])
        #step 12
        done = False
        while done != True:
            CS_cluster_list = []
            for key, value in CS_dic.items():
                CS_cluster_list.append(key)
            if len(CS_cluster_list) >= 2:
                CS_cluster_pair = list(combinations(CS_cluster_list, 2))
                for pair in CS_cluster_pair:
                    distance = cluster_distance(CS_dic[pair[0]], CS_dic[pair[1]])
                    if distance < 2 * math.sqrt(d):
                        for point in CS_dic[pair[1]]['N']:
                            CS_dic[pair[0]]['N'].append(point)
                        CS_dic[pair[0]]['SUM'] = sum_function(CS_dic[pair[0]]['SUM'], CS_dic[pair[1]]['SUM'])
                        CS_dic[pair[0]]['SUMSQ'] = sum_function(CS_dic[pair[0]]['SUMSQ'], CS_dic[pair[1]]['SUMSQ'])
                        CS_dic.pop(pair[1])
                        break
            new_CS_cluster_list = []
            for key, value in CS_dic.items():
                new_CS_cluster_list.append(key)
            if len(new_CS_cluster_list) == len(CS_cluster_list):
                done = True

        if round == 3:
            step13_CS_list = []
            for key, value in CS_dic.items():
                step13_CS_list.append(key)
            #print(step13_CS_list)
            step13_DS_list = []
            for key, value in DS_dic.items():
                step13_DS_list.append(key)
            #step13_DS_list

            for i in step13_CS_list:
                cluster_list = []
                for j in step13_DS_list:
                    distance = cluster_distance(CS_dic[i], DS_dic[j])
                    cluster_list.append((distance, i, j))
                cluster_list.sort()
                min_distance = cluster_list[0][0]
                best_CS_C = cluster_list[0][1]
                best_DS_C = cluster_list[0][2]
                if min_distance < 2 * math.sqrt(d):
                    for point in CS_dic[best_CS_C]['N']:
                        DS_dic[best_DS_C]['N'].append(point)
                    DS_dic[best_DS_C]['SUM'] = sum_function(DS_dic[best_DS_C]['SUM'], CS_dic[best_CS_C]['SUM'])
                    DS_dic[best_DS_C]['SUMSQ'] = sum_function(DS_dic[best_DS_C]['SUMSQ'], CS_dic[best_CS_C]['SUMSQ'])
                    CS_dic.pop(best_CS_C)

        number_DS, number_ccs, number_CS, number_RS = count_number(DS_dic, CS_dic, RS)
        output_file.write(
            "Round " + str(round + 2) + ": " + str(number_DS) + ',' + str(number_ccs) + ',' + str(number_CS) + ',' + str(
                number_RS) + '\n')

    last_dic = {}
    for key, value in DS_dic.items():
        for point in value['N']:
            last_dic[tuple(point)] = key
    for key, value in CS_dic.items():
        for point in value['N']:
            last_dic[tuple(point)] = key
    for point in RS:
        last_dic[tuple(point)] = -1

    #print(len(last_dic))

    file_dic = {}
    for i in x_data:
        file_dic[tuple(i[1])] = i[0]

    answer_list = []
    for key, value in file_dic.items():
        answer_list.append((value, last_dic[key]))

    #answer_list.sort()
    output_file.write("\n")
    output_file.write("The clustering results:\n")
    for i in answer_list:
        output_file.write(str(i[0]) + "," + str(i[1]) + "\n")


    end_time = time.time()
    time = end_time - start_time
    print(time)