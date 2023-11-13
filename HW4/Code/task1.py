import sys
from pyspark import SparkContext
from itertools import combinations
from pyspark.sql import SQLContext
from graphframes import *
import time
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12")

def combination(graph_map, threshold):
    list1 = []
    for i in graph_map.keys():
        list1.append(str(i))
    list2 = []
    for pair in combinations(sorted(list1), 2):
        list2.append(pair)
    edge_list = []
    ver_list = []
    for i in list2:
        set1 = set(graph_map[i[0]])
        set2 = set(graph_map[i[1]])
        interestion = set1.intersection(set2)
        if len(interestion) >= threshold:
            edge_list.append(i)
            edge_list.append((i[1],i[0]))
            if i[0] not in ver_list:
                ver_list.append(i[0])
            if i[1] not in ver_list:
                ver_list.append(i[1])
    return edge_list, ver_list


if __name__ == '__main__':
    start_time = time.time()
    threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(input_file_path)
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)
    train_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1]))

    graph_map = train_data.map(lambda x: (x[0],x[1])).groupByKey().mapValues(list).collectAsMap()
    edge_list, ver_list = combination(graph_map, threshold)
    v_list = []
    for i in ver_list:
        a = (str(i),)
        v_list.append(a)
    #print(sorted(edge_list))
    #print(sorted(v_list))

    sqlContext = SQLContext(sc)
    v = sqlContext.createDataFrame(sorted(v_list), ['id'])
    e = sqlContext.createDataFrame(sorted(edge_list), ['src', 'dst'])
    g = GraphFrame(v, e)
    result = g.labelPropagation(maxIter=5)

    #answer = result.rdd.collect()
    answer = result.rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).map(lambda x: sorted(x[1]))\
        .sortBy(lambda x:(len(x), x)).collect()
    #print(answer)

    file1 = open(output_file_path, "w")
    for i in answer:
        for j in range(len(i)):
            if j != len(i)-1:
                file1.write("'" + i[j] + "'" + ',')
            else:
                file1.write("'" + i[j] + "'")
        file1.write('\n')

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)