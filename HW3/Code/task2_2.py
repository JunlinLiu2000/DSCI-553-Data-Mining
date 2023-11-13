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

def get_B_star(x0, x1, B_review_train_data, U_review_train_data, avg_review_data):
    if B_review_train_data.get(x1) == None and U_review_train_data.get(x0) != None:
        return U_review_train_data[x0]
    elif B_review_train_data.get(x1) != None and U_review_train_data.get(x0) == None:
        return B_review_train_data.get(x1)
    elif B_review_train_data.get(x1) == None and U_review_train_data.get(x0) == None:
        return avg_review_data
    else:
        return B_review_train_data.get(x1)

def get_U_star(x0, x1, B_review_train_data, U_review_train_data,avg_review_data):
    if B_review_train_data.get(x1) == None and U_review_train_data.get(x0) != None:
        return U_review_train_data[x0]
    elif B_review_train_data.get(x1) != None and U_review_train_data.get(x0) == None:
        return B_review_train_data.get(x1)
    elif B_review_train_data.get(x1) == None and U_review_train_data.get(x0) == None:
        return avg_review_data
    else:
        return U_review_train_data.get(x0)

def yelp_B_star(x0,x1,avg_business_rate,avg_user_rate, avg_yelp_star):
    if avg_business_rate.get(x1) == None and avg_user_rate.get(x0) != None:
        return avg_user_rate.get(x0)
    elif avg_business_rate.get(x1) != None and avg_user_rate.get(x0) == None:
        return avg_user_rate.get(x0)
    elif avg_business_rate.get(x1) == None and avg_user_rate.get(x0) == None:
        return avg_yelp_star
    else:
        return avg_business_rate.get(x1)

def yelp_U_star(x0,x1,avg_business_rate,avg_user_rate, avg_yelp_star):
    if avg_business_rate.get(x1) == None and avg_user_rate.get(x0) != None:
        return avg_user_rate.get(x0)
    elif avg_business_rate.get(x1) != None and avg_user_rate.get(x0) == None:
        return avg_user_rate.get(x0)
    elif avg_business_rate.get(x1) == None and avg_user_rate.get(x0) == None:
        return avg_yelp_star
    else:
        return avg_user_rate.get(x0)

def get_avg_checkin(x, checkin_data,avg_checkin):
    if checkin_data.get(x[1]) == None:
        return avg_checkin
    else:
        return checkin_data.get(x[1])

def tip_function(x0,x1,B_tip_data, U_tip_data, avg_tip):
    if B_tip_data.get(x1) == None and U_tip_data.get(x0) != None:
        return avg_user_rate.get(x0)
    elif B_tip_data.get(x1) != None and U_tip_data.get(x0) == None:
        return U_tip_data.get(x0)
    elif B_tip_data.get(x1) == None and U_tip_data.get(x0) == None:
        return avg_tip
    else:
        return B_tip_data.get(x0)

def photo_number_function(x0,x1,photo_business_data, photo_user_data, avg_photo_data):
    if photo_business_data.get(x1) == None and photo_user_data.get(x0) != None:
        return photo_user_data[x0]
    elif photo_business_data.get(x1) == None and photo_user_data.get(x0) == None:
        return avg_photo_data
    else:
        return photo_business_data[x1]

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

    avg_business_rate = train_data.map(lambda x: (x[1], x[2])).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], count_average(x[1]))).collectAsMap()
    avg_user_rate = train_data.map(lambda x: (x[0], x[2])).groupByKey().mapValues(list) \
        .map(lambda x: (x[0], count_average(x[1]))).collectAsMap()
    avg_yelp_star_map = train_data.map(lambda x: float(x[2])).collect()
    avg_yelp_star = statistics.mean(avg_yelp_star_map)

    user_RDD = sc.textFile(folder_path + 'user.json').map(lambda x: json.loads(x))
    business_json_RDD = sc.textFile(folder_path + 'business.json').map(lambda x: json.loads(x))
    #checkin_RDD = sc.textFile(folder_path + 'checkin.json').map(lambda x: json.loads(x))
    #photo_RDD = sc.textFile(folder_path + 'photo.json').map(lambda x: json.loads(x))
    #review_train_RDD = sc.textFile(folder_path + 'review_train.json').map(lambda x: json.loads(x))
    #tip_RDD = sc.textFile(folder_path + 'tip.json').map(lambda x: json.loads(x))

    user_json_data = user_RDD.map(lambda x: (x['user_id'], (x['review_count'], x['average_stars'])))\
        .collectAsMap()
    business_json_data = business_json_RDD.map(lambda x: (x['business_id'], (x['stars'], x['review_count'])))\
        .collectAsMap()
    #B_review_train_data = review_train_RDD.map(lambda x: (x['business_id'], x['stars'])) \
        #.groupByKey().mapValues(list).map(lambda x: (x[0], count_avg2(x[1]))).collectAsMap()
    #U_review_train_data = review_train_RDD.map(lambda x: (x['user_id'], x['stars'])) \
        #.groupByKey().mapValues(list).map(lambda x: (x[0], count_avg2(x[1]))).collectAsMap()
    #avg_review_data = review_train_RDD.map(lambda x: x['stars']).mean()

    #checkin_train_data = checkin_RDD.map(lambda x: (x['business_id'], count_avg1(x['time'])))
    #checkin_data = checkin_train_data.collectAsMap()
    #avg_checkin = checkin_train_data.map(lambda x: x[1]).mean()

    #B_tip_data = tip_RDD.map(lambda x:(x['business_id'],1)).reduceByKey(add).collectAsMap()
    #U_tip_data = tip_RDD.map(lambda x: (x['user_id'], 1)).reduceByKey(add).collectAsMap()
    #avg_tip = statistics.mean(tip_RDD.map(lambda x:(x['business_id'],1)).reduceByKey(add).map(lambda x: float(x[1])).collect())
    #print(tip_data)
    #photo_business_data = photo_RDD.map(lambda x: (x['business_id'], 1)).reduceByKey(add).collectAsMap()
    #photo_user_data = photo_RDD.map(lambda x: (x['photo_id'], 1)).reduceByKey(add).collectAsMap()
    #avg_photo_data = photo_RDD.map(lambda x: (x['business_id'], 1)).reduceByKey(add).map(lambda x:x[1]).mean()

    #tip_list = train_data.map(lambda x: tip_function(x[0],x[1],B_tip_data, U_tip_data, avg_tip)).collect()
    #yelp_bus = train_data.map(lambda x: avg_business_rate.get(x[1])).collect()
    #yelp_user = train_data.map(lambda x: avg_user_rate.get(x[0])).collect()
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

    #B_review_train = train_data.map(lambda x: float(B_review_train_data.get(x[1]))).collect()
    #U_review_train = train_data.map(lambda x: float(U_review_train_data.get(x[0]))).collect()
    #checkin = train_data.map(lambda x: get_avg_checkin(x[1],checkin_data, avg_checkin)).collect()
    #photo_number = train_data.map(
        #lambda x: photo_number_function(x[0], x[1], photo_business_data, photo_user_data, avg_photo_data)).collect()
    x_train_df = pd.DataFrame({'user_count': user_list1, 'user_star': user_list2, 'business_star': list1,
        'business_review_count': list2}) #'B_review_train_star': B_review_train,'yelp_bus':yelp_bus, 'yelp_user': yelp_user})
        #,'yelp_bus':yelp_bus,'checkin_times': checkin, 'tip': tip_list})  # ,'checkin_times': checkin, 'photo_number': photo_number})
    y_train_df = pd.DataFrame({'stars': train_data.map(lambda x: float(x[2])).collect()})

    #print(x_train_df['tip'].isnull().sum())
    #print(x_train_df['user_count'].isnull().sum())
    #print(x_train_df['user_star'].isnull().sum())
    #print(x_train_df['business_star'].isnull().sum())
    #print(x_train_df['B_review_train_star'].isnull().sum())
    #print(x_train_df['U_review_train_star'].isnull().sum())
    #print(x_train_df['yelp_bus'].isnull().sum())
    #print(x_train_df['yelp_bus'].isnull().sum())

    xgbr = xgb.XGBRegressor(max_depth=10, learning_rate=0.01, n_estimators=1000)
    xgbr.fit(x_train_df, y_train_df)

    testRDD = sc.textFile(test_file_path)
    header1 = testRDD.first()
    csv_data1 = testRDD.filter(lambda line: line != header1)
    test_data = csv_data1.map(lambda x: (x.split(',')[0], x.split(',')[1], x.split(',')[2]))

    #test_tip_list = test_data.map(lambda x: tip_function(x[0], x[1], B_tip_data, U_tip_data, avg_tip)).collect()
    #test_yelp_bus = test_data.map(lambda x: yelp_B_star(x[0],x[1],avg_business_rate,avg_user_rate,avg_yelp_star)).collect()
    #test_yelp_user = test_data.map(lambda x: yelp_U_star(x[0],x[1],avg_business_rate,avg_user_rate,avg_yelp_star)).collect()
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
    #test_checkin = test_data.map(lambda x: get_avg_checkin(x[1], avg_checkin)).collect()
    #test_B_review_train = test_data.map(lambda x: get_B_star(x[0], x[1], B_review_train_data, U_review_train_data, avg_review_data)).collect()
    #test_U_review_train = test_data.map(
        #lambda x: get_U_star(x[0], x[1], B_review_train_data, U_review_train_data, avg_review_data)).collect()
    #test_photo_number = test_data.map(
         #lambda x: photo_number_function(x[0], x[1], photo_business_data, photo_user_data, avg_photo_data)).collect()
    x_test_df = pd.DataFrame({'user_count': test_user_list1, 'user_star': test_user_list2, 'business_star': test_list1,
        'business_review_count': test_list2}) #'B_review_train_star': test_B_review_train, 'yelp_bus': test_yelp_bus, 'yelp_user': test_yelp_user})
        #,'yelp_bus':test_yelp_bus, 'checkin_times': test_checkin, 'tip':test_tip_list})

    #print(x_test_df['user_count'].isnull().sum())
    #print(x_test_df['user_star'].isnull().sum())
    #print(x_test_df['business_star'].isnull().sum())
    #print(x_test_df['B_review_train_star'].isnull().sum())
    #print(x_test_df['U_review_train_star'].isnull().sum())
    #print(x_test_df['yelp_bus'].isnull().sum())
    #print(x_test_df['yelp_bus'].isnull().sum())

    y_test_predict = xgbr.predict(x_test_df)
    answer_list = []

    test_data_list = test_data.collect()
    for i in range(len(y_test_predict)):
        answer_list.append([test_data_list[i][0], test_data_list[i][1], y_test_predict[i]])
    header = ['user_id', ' business_id', ' prediction']
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(answer_list)

    a = 0
    b = 0
    for i in range(len(test_data_list)):
        a = a + (test_data_list[i] - y_test_predict[i]) **2
        b = b + 1
    rmse = math.sqrt(a / b)
    print(rmse)

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)