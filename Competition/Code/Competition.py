#Method Description:
# I implemented a hybird recommendation systemn based on Item-based CF recommendation system and Model-based recommendation system(using XGBregressor)
# I used business.josn file and user.json file, and i think review_count, average_stars, useful and so on features have strong related to stars,
# so i selected these features. In order to improve the model, i choose final score = 0.3 * item based score + 0.97 * model based score
# because model based system have a better RMSE than item based system. Also, i performed hyper-parameter tuning like increase the max depth,
# n_estimator, and min_child_weight, choose 0.5 to be the subsample and colsample_bytree.

#Error Distribution:
#>=0 and <1:19
#>=1 and <2:668
#>=2 and <3:11558
#>=3 and <4:80997
#>=4:48802

#RMSE:
#0.978100414440321
#Execution Time:
#560.7513687610626

import sys
from operator import add
import pandas as pd
from pyspark import SparkContext
import time
import csv
import math
import json
import xgboost as xgb
from statistics import mean
import statistics
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

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

def avg_user(a):
    avg_text_rate = user_RDD.map(lambda x: float(x[a])).collect()
    avg_text_rate = mean(avg_text_rate)
    return avg_text_rate

def avg_bus(a):
    avg_text_rate = business_json_RDD.map(lambda x: float(x[a])).collect()
    avg_text_rate = mean(avg_text_rate)
    return avg_text_rate

def sen_analyzer(key, value, list1):
    if key not in list1:
        return (key, 0)
    else:
        analyzer = SentimentIntensityAnalyzer()
        sentiment_dict = analyzer.polarity_scores(value)
    return (key, sentiment_dict['compound'])

def sen_ana(value):
    analyzer = SentimentIntensityAnalyzer()
    sum = 0
    for i in value:
        sentiment_dict = analyzer.polarity_scores(i)
        sum = sum + sentiment_dict['compound']
    avg = sum/len(value)
    if avg < -0.5:
        return 0.5
    elif avg >= -0.5 and avg < 0:
        return 2
    elif avg >= 0 and avg < 0.5:
        return 3
    elif avg >= 0.5:
        return 4.5
    #return avg
def avg_review_train(list1):
    dic1 = {}
    dic2 = {}
    dic3 = {}
    dic4 = {}
    dic5 = {}
    for i in list1:
        if i[0] in dic1:
            dic1[i[0]] = dic1.get(i[0]) + 1
        else:
            dic1[i[0]] = 1
        if i[1] in dic2:
            dic2[i[1]] = dic2.get(i[1]) + 1
        else:
            dic2[i[1]] = 1
        if i[2] in dic3:
            dic3[i[2]] = dic3.get(i[2]) + 1
        else:
            dic3[i[2]] = 1
        if i[3] in dic4:
            dic4[i[3]] = dic4.get(i[3]) + 1
        else:
            dic4[i[3]] = 1
        if i[4] in dic5:
            dic5[i[4]] = dic5.get(i[4]) + 1
        else:
            dic5[i[4]] = 1

    list2 = []
    list3 = []
    list4 = []
    list5 = []
    list6 = []
    for key, value in dic1.items():
        list6.append((value,key))
    for key, value in dic2.items():
        list2.append((value,key))
    for key, value in dic3.items():
        list3.append((value,key))
    for key, value in dic4.items():
        list4.append((value,key))
    for key, value in dic5.items():
        list5.append((value,key))

    list2 = sorted(list2)
    list3 = sorted(list3)
    list4 = sorted(list4)
    list5 = sorted(list5)
    list6 = sorted(list6)

    length = len(list1)
    sum1 = 0
    sum2 = 0
    sum3 = 0
    sum4 = 0
    sum5 = 0
    median_list = []
    for i in list1:
        sum1 = sum1 + i[0]
        sum2 = sum2 + i[1]
        sum3 = sum3 + i[2]
        sum4 = sum4 + i[3]
        sum5 = sum5 + i[4]
        median_list.append(i[2])
    avg1 = sum1 / length
    avg2 = sum2 / length
    avg3 = sum3 / length
    avg4 = sum4 / length
    avg5 = sum5 / length
    median_i1 = statistics.median(median_list)

    return [list6[-1][1], avg2, median_i1, float(list4[-1][1]), float(list5[-1][1])]

def get_noise(dic1):
    if dic1 == None:
        return float(1)
    else:
        noise = dic1.get('NoiseLevel')
        if noise == 'average':
            noise = 3
        elif noise == 'loud':
            noise = 2
        elif noise == 'quiet':
            noise = 4
        elif noise == 'very_loud':
            noise = 1
        else:
            noise = 3
        return noise

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
    user_list = train_data.map(lambda x: x[0]).collect()
    review_train_json_RDD = sc.textFile(folder_path + 'review_train.json').map(lambda x: json.loads(x))
    tip_json_RDD = sc.textFile(folder_path + 'tip.json').map(lambda x: json.loads(x))
    tip_data = tip_json_RDD.map(lambda x: (x['user_id'],x['text'])).groupByKey().mapValues(list)\
        .map(lambda x: (x[0],sen_ana(x[1][0]))).collectAsMap()
    review_train_data1 = review_train_json_RDD.map(lambda x: (x['user_id'],x['text'])).groupByKey().mapValues(list)\
        .map(lambda x: (x[0],sen_ana(x[1][0]))).collectAsMap()

    review_train_data = review_train_json_RDD.map(lambda x: (x['user_id'], (len(x['text']), x['stars'], x['useful'],
                                                                                x['funny'],x['cool']))).groupByKey()\
        .mapValues(list).map(lambda x: (x[0], (avg_review_train(x[1])))).collectAsMap()

    user_RDD = sc.textFile(folder_path + 'user.json').map(lambda x: json.loads(x))
    business_json_RDD = sc.textFile(folder_path + 'business.json').map(lambda x: json.loads(x))

    user_json_data = user_RDD.map(lambda x: (x['user_id'], (x['review_count'], x['average_stars'], x['useful'],
                                                            x['funny'], x['cool'], x['fans'], x['compliment_hot'],
                                                            x['compliment_more'], x['compliment_profile'],
                                                            x['compliment_cute'], x['compliment_list'],
                                                            x['compliment_note'], x['compliment_plain'],
                                                            x['compliment_cool'], x['compliment_funny'],
                                                            x['compliment_writer'],
                                                            x['compliment_photos']))).collectAsMap()

    business_json_data = business_json_RDD.map(
        lambda x: (x['business_id'], (x['stars'], x['review_count'], x['latitude'],
                                      x['longitude'], x['is_open']))).collectAsMap()

    business_noise_data = business_json_RDD.map(lambda x: (x["business_id"], x['attributes'])) \
        .map(lambda x: (x[0], get_noise(x[1]))).collectAsMap()
    noise_json = train_data.map(lambda x: business_noise_data.get(x[1])).collect()
    noise_list = []
    for i in noise_json:
        if i == None:
            noise_list.append(2)
        else:
            noise_list.append(i)

    avg_review_count = 0
    avg_average_stars = 0
    avg_useful = 0
    avg_funny = 0
    avg_compliment_hot = 0
    avg_compliment_cute = 0
    avg_compliment_cool = 0
    avg_compliment_funny = 0
    user_json = train_data.map(lambda x: user_json_data.get(x[0])).collect()
    user_list1 = []
    user_list2 = []
    user_list3 = []
    user_list4 = []
    user_list5 = []
    user_list6 = []
    user_list7 = []
    user_list8 = []
    user_list9 = []
    user_list10 = []
    user_list11 = []
    user_list12 = []
    user_list13 = []
    user_list14 = []
    user_list15 = []
    user_list16 = []
    user_list17 = []
    count = 0
    for i in user_json:
        count = count + 1
        user_list1.append(float(i[0]))
        avg_review_count = avg_review_count + float(i[0])
        user_list2.append(float(i[1]))
        avg_average_stars = avg_average_stars + float(i[1])
        user_list3.append(float(i[2]))
        avg_useful = avg_useful + float(i[2])
        user_list4.append(float(i[3]))
        avg_funny = avg_funny + float(i[3])
        user_list5.append(float(i[4]))
        user_list6.append(float(i[5]))
        user_list7.append(float(i[6]))
        avg_compliment_hot = avg_compliment_hot + float(i[6])
        user_list8.append(float(i[7]))
        user_list9.append(float(i[8]))
        user_list10.append(float(i[9]))
        avg_compliment_cute = avg_compliment_cute + float(i[9])
        user_list11.append(float(i[10]))
        user_list12.append(float(i[11]))
        user_list13.append(float(i[12]))
        user_list14.append(float(i[13]))
        avg_compliment_cool = avg_compliment_cool + float(i[13])
        user_list15.append(float(i[14]))
        avg_compliment_funny = avg_compliment_funny + float(i[14])
        user_list16.append(float(i[15]))
        user_list17.append(float(i[16]))

    avg_review_count = avg_review_count / count
    avg_average_stars = avg_average_stars / count
    avg_useful = avg_useful / count
    avg_funny = avg_funny / count
    avg_compliment_hot = avg_compliment_hot / count
    avg_compliment_cute = avg_compliment_cute / count
    avg_compliment_cool = avg_compliment_cool / count
    avg_compliment_funny = avg_compliment_funny / count

    business_json = train_data.map(lambda x: business_json_data.get(x[1])).collect()
    list1 = []
    list2 = []
    list3 = []
    list4 = []
    list5 = []
    list6 = []
    list7 = []
    avg_stars = 0
    avg_review_count = 0
    avg_latitude = 0
    avg_longitude = 0
    count1 = 0
    for i in business_json:
        list1.append(float(i[0]))
        list2.append(float(i[1]))
        list3.append(float(i[2]))
        list4.append(float(i[3]))
        list5.append(float(i[4]))
        avg_stars = avg_stars + float(i[0])
        avg_review_count = avg_review_count + float(i[1])
        avg_latitude = avg_latitude + float(i[2])
        avg_longitude = avg_longitude + float(i[3])
        count1 = count1 + 1
    avg_stars = avg_stars / count1
    avg_review_count = avg_review_count / count1
    avg_latitude = avg_latitude / count1
    avg_longitude = avg_longitude / count1

    review_train_json = train_data.map(lambda x: review_train_data.get(x[0])).collect()
    review_train_json1 = train_data.map(lambda x: review_train_data1.get(x[0])).collect()
    review_train_list1 = []
    review_train_list2 = []
    review_train_list3 = []
    review_train_list4 = []
    review_train_list5 = []
    for i in review_train_json:
        review_train_list1.append(float(i[0]))
        review_train_list2.append(float(i[1]))
        review_train_list3.append(float(i[2]))
        review_train_list4.append(float(i[3]))
        review_train_list5.append(float(i[4]))
    tip_train_json = train_data.map(lambda x: tip_data.get(x[0])).collect()
    x_train_df = pd.DataFrame({'user_count': user_list1, 'user_star': user_list2,
                               'user_useful': user_list3, 'user_funny': user_list4,
                               'compliment_hot': user_list7, 'compliment_cute': user_list10,
                               'compliment_cool': user_list14, 'compliment_funny': user_list15,
                               'business_star': list1, 'business_review_count': list2,
                               'business_latitude': list3, 'business_longitude': list4,
                               'is_open': list5,
                               'tip': tip_train_json})

    y_train_df = pd.DataFrame({'stars': train_data.map(lambda x: float(x[2])).collect()})
    xgbr = xgb.XGBRegressor(max_depth=10, gamma=1, learning_rate=0.01, n_estimators=1500, min_child_weight=3,
                            subsample=0.5, colsample_bytree=0.5, alpha=1)
    xgbr.fit(x_train_df, y_train_df)

    testRDD = sc.textFile(test_file_path)
    header1 = testRDD.first()
    csv_data1 = testRDD.filter(lambda line: line != header1)
    test_data = csv_data1.map(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[2]))

    test_user_json = test_data.map(lambda x: user_json_data.get(x[0])).collect()
    test_user_list1 = []
    test_user_list2 = []
    test_user_list3 = []
    test_user_list4 = []
    test_user_list5 = []
    test_user_list6 = []
    test_user_list7 = []
    test_user_list8 = []
    test_user_list9 = []
    test_user_list10 = []
    test_user_list11 = []
    test_user_list12 = []
    test_user_list13 = []
    test_user_list14 = []
    test_user_list15 = []
    test_user_list16 = []
    test_user_list17 = []

    for i in test_user_json:
        if i == None:
            test_user_list1.append(float(avg_review_count))
            test_user_list2.append(float(avg_average_stars))
            test_user_list3.append(float(avg_useful))
            test_user_list4.append(float(avg_funny))
            # test_user_list5.append(float(avg_cool))
            # test_user_list6.append(float(avg_fans))
            test_user_list7.append(float(avg_compliment_hot))
            # test_user_list8.append(float(avg_compliment_more))
            # test_user_list9.append(float(avg_compliment_profile))
            test_user_list10.append(float(avg_compliment_cute))
            # test_user_list11.append(float(avg_compliment_list))
            # test_user_list12.append(float(avg_compliment_note))
            # test_user_list13.append(float(avg_compliment_plain))
            test_user_list14.append(float(avg_compliment_cool))
            test_user_list15.append(float(avg_compliment_funny))
            # test_user_list16.append(float(avg_compliment_writer))
            # test_user_list17.append(float(avg_compliment_photos))
        else:
            test_user_list1.append(float(i[0]))
            test_user_list2.append(float(i[1]))
            test_user_list3.append(float(i[2]))
            test_user_list4.append(float(i[3]))
            # test_user_list5.append(float(i[4]))
            # test_user_list6.append(float(i[5]))
            test_user_list7.append(float(i[6]))
            # test_user_list8.append(float(i[7]))
            # test_user_list9.append(float(i[8]))
            test_user_list10.append(float(i[9]))
            # test_user_list11.append(float(i[10]))
            # test_user_list12.append(float(i[11]))
            # test_user_list13.append(float(i[12]))
            test_user_list14.append(float(i[13]))
            test_user_list15.append(float(i[14]))
            # test_user_list16.append(float(i[15]))
            # test_user_list17.append(float(i[16]))

    test_business_json = test_data.map(lambda x: business_json_data.get(x[1])).collect()
    test_list1 = []
    test_list2 = []
    test_list3 = []
    test_list4 = []
    test_list5 = []

    for i in test_business_json:
        if i == None:
            test_list1.append(float(avg_stars))
            test_list2.append(float(avg_review_count))
            test_list3.append(float(avg_latitude))
            test_list4.append(float(avg_longitude))
            test_list5.append(float(1))
        else:
            test_list1.append(float(i[0]))
            test_list2.append(float(i[1]))
            test_list3.append(float(i[2]))
            test_list4.append(float(i[3]))
            test_list5.append(float(i[4]))

    review_test_json = test_data.map(lambda x: review_train_data.get(x[0])).collect()
    review_test_json1 = test_data.map(lambda x: review_train_data1.get(x[0])).collect()
    review_test_list1 = []
    review_test_list2 = []
    review_test_list3 = []
    review_test_list4 = []
    review_test_list5 = []
    #list6 = []
    for i in review_test_json:
        review_test_list1.append(float(i[0]))
        review_test_list2.append(float(i[1]))
        review_test_list3.append(float(i[2]))
        review_test_list4.append(float(i[3]))
        review_test_list5.append(float(i[4]))

    tip_test_json = test_data.map(lambda x: tip_data.get(x[0])).collect()
    x_test_df = pd.DataFrame({'user_count': test_user_list1, 'user_star': test_user_list2,
                              'user_useful': test_user_list3, 'user_funny': test_user_list4,
                              'compliment_hot': test_user_list7, 'compliment_cute': test_user_list10,
                              'compliment_cool': test_user_list14, 'compliment_funny': test_user_list15,
                              'business_star': test_list1, 'business_review_count': test_list2,
                              'business_latitude': test_list3, 'business_longitude': test_list4,
                              'is_open': test_list5,
                              'tip':tip_test_json})

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

    final_score_list = []
    for i in range(len(y_test_predict)):
        final_score = y_test_predict[i] * 0.95 + 0.05 * item_based_predict_y[i][2]
        final_score_list.append(final_score)

    test_data_list = test_data.collect()
    a = 0
    b = 0
    for i in range(len(final_score_list)):
        a = a + (float(test_data_list[i][2]) - float(final_score_list[i])) ** 2
        b = b + 1
    rmse = math.sqrt(a / b)
    print(rmse)

    answer_list = []
    for i in range(len(item_based_predict_y)):
        answer_list.append([item_based_predict_y[i][0], item_based_predict_y[i][1], final_score_list[i]])

    header = ['user_id', ' business_id', ' prediction']
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(answer_list)

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)
    print(avg_stars)
    print(avg_review_count)
    print(avg_latitude)
    print(avg_longitude)
    print(avg_review_count)
    print(avg_average_stars)
    print(avg_useful)
    print(avg_funny)
    print(avg_compliment_hot)
    print(avg_compliment_cute)
    print(avg_compliment_cool)
    print(avg_compliment_funny)
    #review_train_data = review_train_json_RDD.map(lambda x: (x['user_id'], x['text'])).map(lambda x: sen_analyzer(x[0],x[1], user_list)).collectAsMap()
    #print(len(review_train_data))

    # text_list = []
    # review_train_json = train_data.map(lambda x: review_train_data.get(x[0])).collect()
    # for value in review_train_json:
    #     sum_compound = 0
    #     for i in value:
    #         analyzer = SentimentIntensityAnalyzer()
    #         sentiment_dict = analyzer.polarity_scores(i)
    #         sum_compound = sum_compound + sentiment_dict['compound']
    #     avg_compound = sum_compound/len(value)
    #     text_list.append(avg_compound)
    # print(len(text_list))


