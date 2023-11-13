import sys
from itertools import combinations
from pyspark import SparkContext
import random
import time
import csv

def create_matrix(yelp_data):
    user_data = yelp_data.map(lambda x: x[0]).distinct().zipWithIndex().collectAsMap()
    user_business_matrix = yelp_data.map(lambda x: (x[1],user_data[x[0]]))\
        .groupByKey()\
        .map(lambda x: (x[0], sorted(x[1])))\
        .sortByKey()#.collectAsMap()
    length = len(user_data)
    return user_business_matrix, length

def random_select(n):
    prime_number_list = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,
                        103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,
                        199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,283,293,307,311,
                        313,317,331,337,347,349,353,359,367,373,379,383,389,397,401,409,419,421,431,
                        433,439,443,449,457,461,463,467,479,487,491,499,503,509,521,523,541,547,557,
                        563,569,571,577,587,593,599,601,607,613,617,619,631,641,643,647,653,659,661,
                        673,677,683,691,701,709,719,727,733,739,743,751,757,761,769,773,787,797,809,
                        811,821,823,827,829,839,853,857,859,863,877,881,883,887,907,911,919,929,937,
                        941,947,953,967,971,977,983,991,997,1009,1013,1019,1021,1031,1033,1039,1049,
                        1051,1061,1063,1069,1087,1091,1093,1097,1103,1109,1117,1123,1129,1151,1153,
                        1163,1171,1181,1187,1193,1201,1213,1217,1223,1229,1231,1237,1249,1259,1277,
                        1279,1283,1289,1291,1297,1301,1303,1307,1319,1321,1327,1361,1367,1373,1381,
                        1399,1409,1423,1427,1429,1433,1439,1447,1451,1453,1459,1471,1481,1483,1487,
                        1489,1493,1499,1511,1523,1531,1543,1549,1553,1559,1567,1571,1579,1583,1597,
                        1601,1607,1609,1613,1619,1621,1627,1637,1657,1663,1667,1669,1693,1697,1699,
                        1709,1721,1723,1733,1741,1747,1753,1759,1777,1783,1787,1789,1801,1811,1823,
                        1831,1847,1861,1867,1871,1873,1877,1879,1889,1901,1907,1913,1931,1933,1949]
    list1 = []
    for i in range(n):
        index = random.randrange(len(prime_number_list))
        list1.append(prime_number_list[index])
    return list1

def minhash_function(x, band, row, length, a, b):
    n = band * row
    list1 = []
    for i in range(n):
        list1.append(999999)
    for i in list(x):
        for j in range(n):
            new_value = (a[j]*i+b[j]) % length
            if new_value < list1[j]:
                list1[j] = new_value
    return list1

def LSH(data1, data2, band, row):
    bucket = []
    for i in range(band):
        row_index = i * row
        band_list = data2[row_index: row_index + row]
        hash_list = hash(tuple(band_list))
        bucket.append(((hash_list, i), data1))
    return bucket

def combination(x):
    pair_list = []
    for i in combinations(sorted(x), 2):
        pair_list.append(i)
    return pair_list

def count_similarity(x,user_business_map):
    set1 = set(user_business_map[x[0]])
    set2 = set(user_business_map[x[1]])
    union = set1.union(set2)
    intersect = set1.intersection(set2)
    similarity = float(len(intersect) / len(union))
    return [x[0], x[1], similarity]

if __name__ == '__main__':
    start_time = time.time()
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(input_file_path)
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)
    yelp_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1]))

    user_business_matrix, length = create_matrix(yelp_data)
    row = 2
    band = 50
    threshold = 0.5
    a = random_select(row*band)
    b = random_select(row*band)
    signature_matrix = user_business_matrix.mapValues(lambda x: minhash_function(x, row, band, length, a, b))
    #print(signature_matrix.collect())
    user_business_map = user_business_matrix.collectAsMap()
    #print(user_business_map)
    csv_list = signature_matrix.flatMap(lambda x: LSH(x[0], x[1], band, row)).groupByKey().mapValues(list) \
        .filter(lambda x: len(x[1]) >= 2).flatMap(lambda x: combination(x[1])).distinct().sortByKey()
        .map(lambda x: count_similarity(x, user_business_map)) \
        .filter(lambda x: x[2] >= 0.5) \
        .sortBy(lambda x: (x[0], x[1])).collect()
    print(csv_list)
    header = ['business_id_1', ' business_id_2', ' similarity']
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(csv_list)

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)