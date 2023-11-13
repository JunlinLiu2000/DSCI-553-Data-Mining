import sys
from pyspark import SparkContext
from itertools import combinations
import time
from operator import add
import csv
import math
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

def build_graph(edge_list):
    edge_rdd = sc.parallelize(edge_list).distinct()
    graph = edge_rdd.groupByKey().mapValues(list).collectAsMap()
    return graph

def bfs1(root, graph):
    visited = []
    queue = []
    queue.append(root)
    parent_node_level_dic = {}
    parent_node_level_dic[root] = ([], 0)
    while queue:
        cur_node = queue.pop(0)
        visited.append(cur_node)
        for child_node in graph[cur_node]:
            if child_node not in visited:
                visited.append(child_node)
                queue.append(child_node)
                parent_node_level_dic[child_node] = ([cur_node], parent_node_level_dic[1]+1)
            else:
                if parent_node_level_dic.get(child_node)[1] - 1 == parent_node_level_dic.get(cur_node)[1]:
                    parent_node_level_dic[child_node][0].append(cur_node)


def bfs(graph, root):
    visited = []
    queue = []
    level = 0
    path = 1
    p_node_level_path_dic = {}
    p_node_level_path_dic[root] = ([], level, path)
    level_list = [[root]]
    queue.append(root)
    while queue:
        cur_node = queue.pop(0)
        visited.append(cur_node)
        l_list = []
        for child_node in graph[cur_node]:
            if child_node not in visited:
                visited.append(child_node)
                queue.append(child_node)
                p_node_level_path_dic[child_node] = ([cur_node], p_node_level_path_dic[cur_node][1]+1, path)
                l_list.append(child_node)
            else:
                if p_node_level_path_dic.get(child_node)[1] - 1 == p_node_level_path_dic.get(cur_node)[1]:
                    p_node_level_path_dic[child_node][0].append(cur_node)
                    p_node_level_path_dic[child_node] = (p_node_level_path_dic[child_node][0], p_node_level_path_dic[child_node][1], path+1)
        if len(l_list) != 0:
            level_list.append(l_list)
    return p_node_level_path_dic, level_list


def edge_betweenness(x, ppt_graph, ver_list):
    dic, list1 = bfs(ppt_graph, x)
    ver_weight = {}
    #for i in ppt_graph.keys():
        #ver_weight[i] = 1
    for i in ver_list:
        ver_weight[i] = 1

    betweenness_dic = []
    for level in reversed(list1):
        for node in level:
            if dic.get(node) != None:
                for par in dic[node][0]:
                    weight = ver_weight[node] * dic[par][2] / len(dic[node][0])
                    ver_weight[par] += weight
                    key = sorted([node,par])
                    betweenness_dic.append(((key[0], key[1]), weight))
    return betweenness_dic

def sorted_list(x):
    return sorted(x)

if __name__ == '__main__':
    start_time = time.time()
    threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    bw_output_path = sys.argv[3]
    cm_output_path = sys.argv[4]
    sc = SparkContext('local[*]', 'task2')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(input_file_path)
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)
    train_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1]))

    graph_map = train_data.map(lambda x: (x[0],x[1])).groupByKey().mapValues(list).collectAsMap()
    edge_list, ver_list = combination(graph_map, threshold)
    #graph = build_graph(edge_list)
    #print(ver_list)
    node_betweenness = sc.parallelize(ver_list).map(lambda x: edge_betweenness(x, graph_map, ver_list)) \
        .flatMap(lambda x: sorted_list(x)).reduceByKey(add).map(lambda x: (x[0], round(x[1]/2, 5))) \
        .sortBy(lambda x: -x[1]).collect()
    #print(node_betweenness)

    #w_dic = {}
    #for i in node_betweenness:
        #for j in i:
            #if j[0] not in w_dic:
                #w_dic[j[0]] = j[1]/2
            #else:
                #w_dic[j[0]] = w_dic[j[0]] + j[1]/2
    #ans_list = []
    #for key in sorted(w_dic):
        #ans_list.append((key, w_dic[key]))
    #print(ans_list)
    #ans_list.sort(key = lambda x: x[1], reverse= True)
    #print(ans_list)
    file1 = open(bw_output_path, "w")
    for i in node_betweenness:
        id = i[0]
        bw = i[1]
        file1.write(str(id) + ',' + str(bw))
        file1.write('\n')

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)
