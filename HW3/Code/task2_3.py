import sys
from operator import add
import pandas as pd
from pyspark import SparkContext
import time
import csv
import math
import json
import xgboost as xgb
import statistics

def count_average(x):
    sum = 0
    if len(list(x)) == 0:
        return 0
    for i in list(x):
        sum = sum + float(i)
    average = float(sum/len(list(x)))
    return average

def count_avg1(x):
    sum = 0
    length = len(x)
    if length == 0:
        return 0
    for key, value in x.items():
        sum = sum + value
    avg_checkin = float(sum/length)
    return avg_checkin

def count_avg2(x):
    return float(sum(x)/len(x))

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
    folder_path = sys.argv[1]
    test_file_path = sys.argv[2]
    output_file_path = sys.argv[3]
    sc = SparkContext('local[*]', 'task2_1')
    sc.setLogLevel("WARN")

    textRDD = sc.textFile(folder_path + 'yelp_train.csv')
    header = textRDD.first()
    csv_data = textRDD.filter(lambda line: line != header)
    train_data = csv_data.map(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[2]))

    user_RDD = sc.textFile(folder_path + 'user.json').map(lambda x: json.loads(x))
    business_json_RDD = sc.textFile(folder_path + 'business.json').map(lambda x: json.loads(x))

    user_json_data = user_RDD.map(lambda x: (x['user_id'], (x['review_count'], x['average_stars'])))\
        .collectAsMap()
    business_json_data = business_json_RDD.map(lambda x: (x['business_id'], (x['stars'], x['review_count'])))\
        .collectAsMap()

    user_json = train_data.map(lambda x: user_json_data.get(x[0])).collect()
    user_list1 = []
    user_list2 = []
    for i in user_json:
        user_list1.append(float(i[0]))
        user_list2.append(float(i[1]))

    business_json = train_data.map(lambda x: business_json_data.get(x[1])).collect()
    list1 = []
    list2 = []
    for i in business_json:
        list1.append(float(i[0]))
        list2.append(float(i[1]))

    x_train_df = pd.DataFrame({'user_count': user_list1, 'user_star': user_list2, 'business_star': list1, 'business_review_count': list2})
    y_train_df = pd.DataFrame({'stars': train_data.map(lambda x: float(x[2])).collect()})

    xgbr = xgb.XGBRegressor(max_depth=10, learning_rate=0.01, n_estimators=1000)
    xgbr.fit(x_train_df, y_train_df)

    testRDD = sc.textFile(test_file_path)
    header1 = testRDD.first()
    csv_data1 = testRDD.filter(lambda line: line != header1)
    test_data = csv_data1.map(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[2]))

    test_user_json = test_data.map(lambda x: user_json_data.get(x[0])).collect()
    test_user_list1 = []
    test_user_list2 = []
    for i in test_user_json:
        test_user_list1.append(float(i[0]))
        test_user_list2.append(float(i[1]))

    test_business_json = test_data.map(lambda x: business_json_data.get(x[1])).collect()
    test_list1 = []
    test_list2 = []
    for i in test_business_json:
        test_list1.append(float(i[0]))
        test_list2.append(float(i[1]))

    x_test_df = pd.DataFrame({'user_count': test_user_list1, 'user_star': test_user_list2, 'business_star': test_list1,'business_review_count': test_list2})
    y_test_predict = xgbr.predict(x_test_df)

    # Item-based CF
    avg_business_rate = train_data.map(lambda x: (x[1], x[2])).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], count_average(x[1]))).collectAsMap()
    # print(avg_business_rate)
    avg_user_rate = train_data.map(lambda x: (x[0], x[2])).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], count_average(x[1]))).collectAsMap()
    # print(avg_user_rate)
    B_U_R_data = train_data.map(lambda x: (x[1], (x[0], x[2]))).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], dict(x[1]))).collectAsMap()
    # print(B_U_R_data)
    U_B_R_data = train_data.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], dict(x[1]))).collectAsMap()
    # print(U_B_R_data)
    business_data_map = train_data.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list).collectAsMap()
    # print(business_data_map)
    user_data_map = train_data.map(lambda x: (x[0], x[1])).groupByKey().mapValues(list).collectAsMap()

    predict_pair1 = test_data.map(lambda x: (x[0], x[1], predict(x[0], x[1], U_B_R_data, B_U_R_data, avg_user_rate,
                                                                 avg_business_rate, business_data_map, user_data_map)))

    item_based_predict_y = predict_pair1.collect()

    #final_score_list = []
    #for i in range(len(y_test_predict)):
        #final_score = y_test_predict[i]*0.95 + 0.05 * item_based_predict_y[i][2]
        #final_score_list.append(final_score)
    zero_one = 0
    one_two = 0
    two_three = 0
    three_four = 0
    four = 0

    final_score_list = []
    for i in range(len(y_test_predict)):
        final_score = y_test_predict[i]*0.97 + 0.03 * item_based_predict_y[i][2]
        final_score_list.append(final_score)
        if final_score >= 0 and final_score < 1:
            zero_one = zero_one + 1
        elif final_score >= 1 and final_score < 2:
            one_two = one_two + 1
        elif final_score >= 2 and final_score < 3:
            two_three = two_three + 1
        elif final_score >= 3 and final_score < 4:
            three_four = three_four + 1
        elif final_score >= 4:
            four = four + 1

    test_data_list = test_data.collect()
    a = 0
    b = 0
    for i in range(len(final_score_list)):
        a = a + (float(test_data_list[i][2]) - float(final_score_list[i])) ** 2
        b = b + 1
    rmse = math.sqrt(a / b)
    print(rmse)
    print(zero_one)
    print(one_two)
    print(two_three)
    print(three_four)
    print(four)


    answer_list = []
    for i in range(len(item_based_predict_y)):
        answer_list.append([item_based_predict_y[i][0],item_based_predict_y[i][1],final_score_list[i]])

    header = ['user_id', ' business_id', ' prediction']
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(answer_list)

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)