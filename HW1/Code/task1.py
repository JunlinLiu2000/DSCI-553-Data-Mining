import json
import sys
from operator import add
from pyspark import SparkConf, SparkContext

# A.The total number of reviews
def number_reviews(all_data):
    number_review = all_data.count()
    return number_review

# B.The number of reviews in 2018
def number_reviews_2018(all_data):
    total_number = all_data.filter(lambda line: str(2018) in line["date"]).count()
    return total_number

# C.The number of distinct users who wrote reviews
def dist_users_reviews(all_data):
    total_dist_number = all_data.map(lambda line: line['user_id']).distinct().count()
    return total_dist_number

# D.The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
def top_users_reviews(all_data):
    top_10_review = all_data.map(lambda line: (line['user_id'], 1)).reduceByKey(add) \
        .sortBy(lambda x: (-x[1], x[0]))
    top_10_review1 = top_10_review.collect()[:10]
    print(top_10_review1)
    #dic1 ={}
    #for element in top_10_review1:
        #if element[1] in dic1.keys():
            #dic1[element[1]].append(element[0])
        #else:
            #dic1[element[1]] = [element[0]]
    #sorted_list = []
    #for key, value in dic1.items():
        #for element in sorted(value):
            #sorted_list.append((element, key))
    #sorted_list = sorted_list.sort(key=lambda x: x[1])

    #largest_number = top_10_review.collect()[-1][1]
    #answer_list = []
    #for element in top_10_review.collect():
        #if largest_number in element:
            #answer_list.append(element)
    #answer_list.sort(key=lambda x: x[0])
    #answer_list = sorted_list
    return top_10_review1

# E.The number of distinct businesses that have been reviewed
def dist_business(all_data):
    total_dist_business = all_data.map(lambda line: line['business_id']).distinct().count()
    return total_dist_business

# F.The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
def top_business_reviews(all_data):
    top_10_businessreview = all_data.map(lambda line: (line['business_id'], 1)).reduceByKey(add) \
        .sortBy(lambda x: x[1])
    largest_number = top_10_businessreview.collect()[-1][1]
    answer_list = []
    for element in top_10_businessreview.collect():
        if largest_number in element:
            answer_list.append(element)
    answer_list.sort(key=lambda x: x[0])
    answer_list = answer_list[:10]
    return answer_list




if __name__ == '__main__':
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel("ERROR")
    textRDD = sc.textFile(input_file_path)
    all_data = textRDD.map(lambda line: json.loads(line))
    #print(all_data.collect())

    top_users_reviews(all_data)
    answer = {}
    answer['n_review'] = number_reviews(all_data)
    answer['n_review_2018'] = number_reviews_2018(all_data)
    answer['n_user'] = dist_users_reviews(all_data)
    answer['top10_user'] = top_users_reviews(all_data)
    answer['n_business'] = dist_business(all_data)
    answer['top10_business'] = top_business_reviews(all_data)

    with open(output_file_path, 'w') as outputfile1:
        json.dump(answer, outputfile1)