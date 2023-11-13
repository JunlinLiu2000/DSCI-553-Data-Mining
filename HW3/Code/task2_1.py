import sys
from pyspark import SparkContext
import time
import csv
import math

def count_average(x):
    sum = 0
    if len(list(x)) == 0:
        return 0
    for i in list(x):
        sum = sum + float(i)
    average = float(sum/len(list(x)))
    return average

def pearson_similarity(b1, b2, business_data_map, B_U_R_data, avg_b_rate):
    if avg_b_rate.get(b1) == None or avg_b_rate.get(b2) == None:
        return 0
    avg_b1 = float(avg_b_rate[b1])
    avg_b2 = float(avg_b_rate[b2])

    b1_user_list = set(business_data_map[b1])
    b2_user_list = set(business_data_map[b2])
    intersection_list = b1_user_list.intersection(b2_user_list)
    Numerator = 0
    b1_Denominator = 0
    b2_Denominator = 0
    for i in intersection_list:
        Numerator = Numerator + (float(B_U_R_data[b1][i])-avg_b1) * (float(B_U_R_data[b2][i])-avg_b2)
        b1_Denominator = b1_Denominator + ((float(B_U_R_data[b1][i]) - avg_b1) ** 2)
        b2_Denominator = b2_Denominator + ((float(B_U_R_data[b2][i]) - avg_b2) ** 2)
    if b1_Denominator * b2_Denominator == 0:
        similarity = 0
    else:
        similarity = float(Numerator / (math.sqrt(b1_Denominator) * math.sqrt(b2_Denominator)))
    return similarity

def predict(u1,b1,U_B_R_data,B_U_R_data, avg_user_rate, avg_business_rate, business_data_map, user_data_map):
    user_id = u1
    business_id = b1
    if user_data_map.get(u1) == None and business_data_map.get(b1) != None:
        return avg_business_rate[b1]
    if user_data_map.get(u1) != None and business_data_map.get(b1) == None:
        return avg_user_rate[u1]
    if user_data_map.get(u1) == None and business_data_map.get(b1) == None:
        return 0

    bus_list = []
    for i in U_B_R_data[u1].keys():
        if i != b1:
            bus_list.append((b1,i))
    if len(bus_list) <= 60:
        return avg_business_rate[b1]

    similarity_list = []
    for i in bus_list:
        similarity = pearson_similarity(i[0], i[1], business_data_map, B_U_R_data, avg_business_rate)
        if similarity > 0:
            similarity_list.append((i,similarity))

    Numerator = 0
    Denominator = 0
    for i in similarity_list:
        rate = float(U_B_R_data[user_id][i[0][1]])
        Numerator = Numerator + float(rate * float(i[1]))
        Denominator = Denominator + float(i[1])
    if Numerator == 0 or Denominator == 0:
        predict = avg_business_rate[business_id]
    else:
        predict = float(Numerator / Denominator)
    return predict

def rmse(x):
    Numerator = 0
    Denominator = 0
    Numerator = Numerator + (x[0]-x[1]) ** 2
    Denominator = Denominator + 1
    return Numerator, Denominator

if __name__ == '__main__':
    start_time = time.time()
    input_file_path = sys.argv[1]
    test_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    sc = SparkContext('local[*]', 'task2_1')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(input_file_path)
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)
    train_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[2]))

    avg_business_rate = train_data.map(lambda x: (x[1], x[2])).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], count_average(x[1]))).collectAsMap()
    #print(avg_business_rate)
    avg_user_rate = train_data.map(lambda x: (x[0], x[2])).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], count_average(x[1]))).collectAsMap()
    #print(avg_user_rate)
    B_U_R_data = train_data.map(lambda x: (x[1], (x[0], x[2]))).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], dict(x[1]))).collectAsMap()
    #print(B_U_R_data)
    U_B_R_data = train_data.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], dict(x[1]))).collectAsMap()
    #print(U_B_R_data)
    business_data_map = train_data.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).collectAsMap()
    #print(business_data_map)
    user_data_map = train_data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(list).collectAsMap()
    #print(user_data_map)

    testRDD = sc.textFile(test_file_path)
    header1 = testRDD.first()
    csv_data1 = testRDD.filter(lambda line: line != header1)
    test_data = csv_data1.filter(lambda line: line != header) \
        .map(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[2]))
    predict_pair1 = test_data.map(lambda x: (x[0], x[1], predict(x[0], x[1], U_B_R_data, B_U_R_data, avg_user_rate, avg_business_rate, business_data_map, user_data_map)))

    predict_pair = test_data.map(lambda x: (x[0], x[1], (float(x[2]), predict(x[0], x[1], U_B_R_data, B_U_R_data, avg_user_rate, avg_business_rate, business_data_map, user_data_map))))

    rmse_list = predict_pair.map(lambda x: rmse(x[2])).collect()
    Numerator = 0
    Denominator = 0
    for i in rmse_list:
        Numerator = Numerator + i[0]
        Denominator = Denominator + 1
    rmse = math.sqrt(Numerator/Denominator)
    print(rmse)

    header = ['user_id', ' business_id', ' prediction']
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(predict_pair1.collect())

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)