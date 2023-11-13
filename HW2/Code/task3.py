import sys
from operator import add
from itertools import combinations
from pyspark import SparkContext

def single_candidates(csv_data):
    candidates = csv_data.map(lambda line: line.split(',')[1]) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add).collect()

    single_candidates_list = []
    for i in sorted(candidates):
        if i[1] >= int(support):
            single_candidates_list.append(i[0])

    return single_candidates_list

def find_support_candidates(support, candidates):
    candidates_list = []

    for i in sorted(candidates):
        if i[1] >= int(support):
            candidates_list.append(i[0])

    return sorted(candidates_list)

def item_combination(tuple_size, candidates, csv_data):
    combinations_list = []
    for i in combinations(candidates, tuple_size):
        combinations_list.append(set(i))

    all_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1])) \
        .groupByKey().mapValues(list) \
        .sortBy(lambda x: x[0]) \
        .map(lambda x: set(x[1])).collect()
    #tuple1 = [tuple(i) for i in all_data]
    tuple1 = all_data
    #print(tuple1)

    dic = {}
    for i in combinations_list:
        for j in tuple1:
            if i.issubset(j):
                dic[tuple(i)] = dic.get(tuple(i), 0) + 1
    #print(dic)
    new_candidates = []
    for key, value in dic.items():
        if value >= tuple_size:
            new_candidates.append(set(key))
    print(new_candidates)
    return new_candidates

def pair_combination(new_candidates, tuple_size):
    list1 = new_candidates[0]
    for i in new_candidates[1:]:
        list1 = list1.union(i)

    combinations_list = []
    if len(list1) < tuple_size:
        for i in combinations(list1, tuple_size):
            combinations_list.append(set(i))

    all_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1])) \
        .groupByKey().mapValues(list) \
        .sortBy(lambda x: x[0]) \
        .map(lambda x: set(x[1])).collect()

    tuple1 = all_data
    dic = {}
    for i in combinations_list:
        for j in tuple1:
            if i.issubset(j):
                dic[tuple(i)] = dic.get(tuple(i), 0) + 1

    new_candidates = []
    for key, value in dic.items():
        if value >= tuple_size:
            new_candidates.append(set(key))

    return new_candidates

def find_all_candidates(csv_data):
    candidates_list = []
    tuple_size = 2
    single_candidates_list = single_candidates(csv_data)
    while tuple_size != len(single_candidates_list):
        new_candidates = pair_combination(single_candidates_list, tuple_size)
        single_candidates_list = new_candidates
        candidates_list.append(new_candidates)
        tuple_size = tuple_size + 1
    print(single_candidates_list)



if __name__ == '__main__':
    case_number = sys.argv[1]
    support = sys.argv[2]
    input_file_path = sys.argv[3]
    #output_file_path = sys.argv[4]
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(input_file_path)
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)

    tuple_size = int(2)
    single_candidates_list = single_candidates(csv_data)
    new_candidates = item_combination(tuple_size, single_candidates_list, csv_data)
    k=3
    #list1 = pair_combination(new_candidates, k)
    print(new_candidates)