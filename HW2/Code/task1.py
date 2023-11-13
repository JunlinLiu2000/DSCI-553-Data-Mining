import sys
from operator import add
from itertools import combinations
from pyspark import SparkContext
import math
import time

def all_single_candidates(csv_data):
    candidates = csv_data.map(lambda line: line.split(',')[1]) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add).collect()
    return candidates

def all_single_user(csv_data):
    candidates = csv_data.map(lambda line: line.split(',')[0]) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add).collect()
    return candidates

def union_function(single_candidates_list):
    list1 = single_candidates_list[0]
    for i in single_candidates_list[1:]:
        list1 = list1.union(i)
    return list1

def pair_combination(listdata, single_candidates_list, tuple_size, threshold):
    dic = {}
    list1 = union_function(single_candidates_list)
    for data in listdata:
        setdata = set(data)
        #print(setdata)
        data = setdata.intersection(list1)
        for i in combinations(sorted(data), tuple_size):
            dic[i] = dic.get(i, 0) + 1

    new_candidates = []
    for key, value in dic.items():
        if value >= threshold:
            new_candidates.append(set(key))
    #print(new_candidates)
    return new_candidates

def find_all_candidates(x, data_size, support, candidates):
    listdata = list(x)
    threshold = math.ceil((len(listdata) / data_size) * support)
    #print(len(listdata))
    candidates_list = []
    single_candidates_list = []
    #print(f"throdld is :", threshold)
    for i in sorted(candidates):
        if i[1] >= threshold:
            single_candidates_list.append({i[0]})
    #print(f"single:",single_candidates_list)
    for i in single_candidates_list:
        candidates_list.append(tuple(i))

    single_len = len(single_candidates_list)
    tuple_size = 2
    while tuple_size != single_len + 1:
        new_candidates = pair_combination(listdata, single_candidates_list, tuple_size, threshold)
        if len(new_candidates) == 0:
            break
        else:
            single_candidates_list = new_candidates
            #print(single_candidates_list)
            for i in single_candidates_list:
                print(i)
                if i in candidates_list:
                    continue
                candidates_list.append(tuple(sorted(i)))
        tuple_size = tuple_size + 1

    return sorted(candidates_list)

def find_frequent(x, candidates, support):
    listdata = list(x)
    dic ={}
    for i in listdata:
        for j in candidates:
            if set(j).issubset(set(i)):
                dic[tuple(j)] = dic.get(tuple(j), 0) + 1

    frequent_list = []
    for key, value in dic.items():
        #if value >= support:
        frequent_list.append((key, value))
    #print(frequent_list)
    return frequent_list

if __name__ == '__main__':
    start_time = time.time()

    case_number = sys.argv[1]
    support = sys.argv[2]
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(input_file_path)
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)

    single_candidates = all_single_candidates(csv_data)
    single_user = all_single_user(csv_data)
    #print(single_candidates)
    support = int(support)

    if int(case_number) == 1:
        business_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1])) \
            .groupByKey().mapValues(list) \
            .sortBy(lambda x: x[0]) \
            .map(lambda x: set(x[1]))
        #print(business_data.collect())
        data_size = business_data.count()
        #print(f"all data", all_data)
        candidates = business_data.mapPartitions(lambda x: find_all_candidates(x, data_size, support, single_candidates)) \
            .distinct().sortBy(lambda x: (len(x), x)).collect()
        #print(candidates)
        fre_list = business_data.mapPartitions(lambda x: find_frequent(x, candidates, support)) \
            .reduceByKey(add) \
            .filter(lambda x: x[1]>=support) \
            .map(lambda x: x[0]) \
            .sortBy(lambda x: (len(x), x)).collect()
        #print(fre_list)
    else:
        user_data = csv_data.map(lambda x: (x.split(',')[1], x.split(',')[0])) \
            .groupByKey().mapValues(list) \
            .sortBy(lambda x: x[0]) \
            .map(lambda x: set(x[1]))
        data_size = user_data.count()
        candidates = user_data.mapPartitions(lambda x: find_all_candidates(x, data_size, support, single_user)) \
            .distinct().sortBy(lambda x: (len(x), x)).collect()
        fre_list = user_data.mapPartitions(lambda x: find_frequent(x, candidates, support)) \
            .reduceByKey(add) \
            .filter(lambda x: x[1] >= support) \
            .map(lambda x: x[0]) \
            .sortBy(lambda x: (len(x), x)).collect()

    sorted_candidates = candidates
    #print(sorted_candidates)
    a = 1
    change_list = []
    for i in range(1, len(sorted_candidates)):
        if len(sorted_candidates[i]) > a:
            change_list.append(i)
            a = len(sorted_candidates[i])
    change_list.append(-1)

    file1 = open(output_file_path, "w")
    file1.write('Candidates: \n')

    x = 1
    for i in range(0, len(sorted_candidates)):
        if len(sorted_candidates[i]) > a:
            change_list.append(i)
            a = len(sorted_candidates[i])
    change_list.append(-1)

    for i in range(0, len(sorted_candidates)):
        if i < change_list[0]-1:
            file1.write("('"+str(sorted_candidates[i][0])+"')" +",")
            continue
        if i == change_list[0]-1:
            file1.write("('" + str(sorted_candidates[i][0]) + "')" +'\n\n')
            continue
        if i == change_list[x]:
            file1.write('\n\n')
            x = x + 1
        if i+1 == change_list[x]:
            file1.write(str(sorted_candidates[i]))
            continue
        if i == len(sorted_candidates)-1:
            file1.write(str(sorted_candidates[i]) + '\n\n')
            break
        file1.write(str(sorted_candidates[i]) + ',')

    a = 1
    change_list = []
    for i in range(0, len(fre_list)):
        if len(fre_list[i]) > a:
            change_list.append(i)
            a = len(fre_list[i])
    change_list.append(-1)

    x = 1
    file1.write('Frequent Itemsets: \n')
    for i in range(0, len(fre_list)):
        if i < change_list[0]-1:
            file1.write("('" + str(fre_list[i][0]) + "')" + ",")
            continue
        if i == change_list[0]-1:
            file1.write("('" + str(fre_list[i][0]) + "')"+'\n\n')
            continue
        if i == change_list[x]:
            file1.write('\n\n')
            x = x + 1
        if i+1 == change_list[x]:
            file1.write(str(fre_list[i]))
            continue
        if i == len(fre_list)-1:
            file1.write(str(fre_list[i]))
            break
        file1.write(str(fre_list[i]) + ',')

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)
