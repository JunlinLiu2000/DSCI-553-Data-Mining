import csv
import json
import sys
from operator import add
from pyspark import SparkConf, SparkContext

def average_satrs(review_filepath, business_filepath):
   textRDD1 = sc.textFile(review_filepath)
   review_file = textRDD1.map(lambda line: json.loads(line))
   textRDD2 = sc.textFile(business_filepath)
   business_file = textRDD2.map(lambda line: json.loads(line))

   review_data = review_file.map(lambda line: (line['business_id'], line['stars']))
   business_data = business_file.map(lambda line: (line['business_id'], line['city']))
   count_number = business_data.rightOuterJoin(review_data).map(lambda line: (line[1][0], 1)) \
   .reduceByKey(add)

   join_data = business_data.rightOuterJoin(review_data).map(lambda line: (line[1][0], line[1][1])) \
   .reduceByKey(lambda x, y: x + y)

   final_data = count_number.join(join_data).map(lambda line: (line[0], line[1][1]/line[1][0]))\
      .map(lambda x : x).sortBy(lambda x: x[1]) \
      .sortBy(lambda x: (-x[1], x[0]))
   answer_list =[]
   for element in final_data.collect():
      answer_list.append([element[0],element[1]])
   return answer_list

def spark_method(review_filepath, business_filepath):
   import time
   start_time = time.time()

   textRDD1 = sc.textFile(review_filepath)
   review_file = textRDD1.map(lambda line: json.loads(line))
   textRDD2 = sc.textFile(business_filepath)
   business_file = textRDD2.map(lambda line: json.loads(line))

   review_data = review_file.map(lambda line: (line['business_id'], line['stars']))
   business_data = business_file.map(lambda line: (line['business_id'], line['city']))
   count_number = business_data.rightOuterJoin(review_data).map(lambda line: (line[1][0], 1)) \
      .reduceByKey(add)

   join_data = business_data.rightOuterJoin(review_data).map(lambda line: (line[1][0], line[1][1])) \
      .reduceByKey(lambda x, y: x + y)

   final_data = count_number.join(join_data).map(lambda line: (line[0], line[1][1] / line[1][0])) \
      .map(lambda x: x).sortBy(lambda x: x[1]) \
      .sortBy(lambda x: (-x[1], x[0])).take(10)

   city_list = []
   for element in final_data:
      city_list.append(element[0])
   print(city_list)
   end_time = time.time()
   time = end_time - start_time
   return time



def python_method(review_filepath, business_filepath):
   import time
   start_time = time.time()

   textRDD1 = sc.textFile(review_filepath)
   review_file = textRDD1.map(lambda line: json.loads(line))
   textRDD2 = sc.textFile(business_filepath)
   business_file = textRDD2.map(lambda line: json.loads(line))

   review_data = review_file.map(lambda line: (line['business_id'], line['stars']))
   business_data = business_file.map(lambda line: (line['business_id'], line['city']))
   count_number = business_data.rightOuterJoin(review_data).map(lambda line: (line[1][0], 1)) \
      .reduceByKey(add)

   join_data = business_data.rightOuterJoin(review_data).map(lambda line: (line[1][0], line[1][1])) \
      .reduceByKey(lambda x, y: x + y)

   final_data = count_number.join(join_data).map(lambda line: (line[0], line[1][1] / line[1][0]))
   answer_list = final_data.collect()
   answer_list.sort(reverse = True, key=lambda x:x[1])

   data_dic = {}
   for element in answer_list:
      if element[1] in data_dic.keys():
         data_dic[element[1]].append(element[0])
      else:
         data_dic[element[1]]=[element[0]]

   final_list = []
   for key, value in data_dic.items():
      for element in sorted(value):
         final_list.append(element)

   print(final_list[:10])
   end_time = time.time()
   time = end_time - start_time
   return time


if __name__ == '__main__':
   review_filepath = sys.argv[1]
   business_filepath = sys.argv[2]
   output_file_path_a = sys.argv[3]
   output_file_path_b = sys.argv[4]
   sc = SparkContext('local[*]', 'task3')
   sc.setLogLevel("ERROR")

   column_name =['city','stars']
   answer_list = average_satrs(review_filepath, business_filepath)
   with open(output_file_path_a, 'w') as outputfile3:
      file = csv.writer(outputfile3)
      file.writerow(column_name)
      file.writerows(answer_list)

   spark_time = spark_method(review_filepath, business_filepath)
   python_time = python_method(review_filepath, business_filepath)

   time_dic = {}
   time_dic['m1'] = python_time
   time_dic['m2'] = spark_time
   time_dic['reason'] = 'The data we are using is small, and it could be keep in computer memory to avoid the I/O for python. Also, the number of defult partition for spark is 160 in Vocareum, and it is too big for sorting small dataset. In this case, runtime of python is less than the run time of Spark'
   with open(output_file_path_b, 'w') as outputfile4:
      json.dump(time_dic, outputfile4)