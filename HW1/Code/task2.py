import json
import sys
import time
from operator import add
from pyspark import SparkConf, SparkContext

def hash_domain(x):
   return hash(x[0])

def default_partition(input_file_path):
   import time
   start_time = time.time()

   textRDD = sc.textFile(input_file_path)
   all_data = textRDD.map(lambda line: json.loads(line))
   top_10_businessreview = all_data.map(lambda line: (line['business_id'], 1)).reduceByKey(add) \
      .sortBy(lambda x: (-x[1], x[0]))
   top_10_businessreview1 = top_10_businessreview.collect()[-10:]
   print(top_10_businessreview1)
   #largest_number = top_10_businessreview.collect()[-1][1]
   #answer_list = []
   #for element in top_10_businessreview.collect():
      #if largest_number in element:
         #answer_list.append(element)
   #answer_list.sort(key=lambda x: x[0])
   #answer_list = answer_list[:10]

   number_partition = top_10_businessreview.getNumPartitions()
   items_number = top_10_businessreview.glom().map(lambda x: len(x)).collect()

   #print(top_10_businessreview.glom().collect())
   end_time = time.time()
   time = end_time - start_time
   list2= [number_partition, items_number, time]
   return list2

def customized_partition(input_file_path, n_partition):
   import time
   start_time = time.time()

   textRDD = sc.textFile(input_file_path)
   all_data = textRDD.map(lambda line: json.loads(line))
   top_10_businessreview = all_data.map(lambda line: (line['business_id'], 1)).partitionBy(int(n_partition), hash_domain) \
      .reduceByKey(add) \
      .sortBy(lambda x: (-x[1], x[0]))
   top_10_businessreview1 = top_10_businessreview.collect()[-10:]

   items_number = top_10_businessreview.glom().map(lambda x: len(x)).collect()
   #print(top_10_businessreview.glom().collect())
   end_time = time.time()
   time = end_time - start_time
   list2 = [items_number, time]
   return list2

if __name__ == '__main__':
   input_file_path = sys.argv[1]
   output_file_path = sys.argv[2]
   n_partition = sys.argv[3]
   sc = SparkContext('local[*]', 'task2')
   sc.setLogLevel("ERROR")
   #textRDD = sc.textFile(input_file_path)
   #all_data = textRDD.map(lambda line: json.loads(line))

   answer = {}
   default = {}
   customized = {}

   default_list = default_partition(input_file_path)
   customized_list = customized_partition(input_file_path, n_partition)
   default['n_partition'] = default_list[0]
   default['n_items'] = default_list[1]
   default['exe_time'] = default_list[2]
   customized['n_partition'] = n_partition
   customized['n_items'] = customized_list[0]
   customized['exe_time'] = customized_list[1]
   answer['default'] = default
   answer['customized'] = customized

   with open(output_file_path, 'w') as outputfile2:
      json.dump(answer, outputfile2)