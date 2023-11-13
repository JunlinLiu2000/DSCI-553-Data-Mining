import sys
import time
import binascii
from operator import add
from blackbox import BlackBox
import random
import csv
from statistics import median

def myhash(s):
    result = []
    number_hash = 10
    hash_function_list = hash_function(number_hash, s)
    for f in hash_function_list:
        result.append(f)
    return result

def hash_function(number_hash, s):
    a = random.sample(range(1, 99999), number_hash)
    b = random.sample(range(1, 99999), number_hash)
    list1 = []
    for i in range(number_hash):
        x = int(binascii.hexlify(s.encode('utf8')), 16)
        hash_val = (a[i] * x + b[i]) % length
        list1.append(hash_val)
    return list1

if __name__ == '__main__':
    start_time = time.time()
    input_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_file_path = sys.argv[4]

    bx = BlackBox()
    stream_users = bx.ask(input_path, stream_size)
    #print(stream_users)
    length = 69997
    filter_bit_array = []
    for i in range(length):
        filter_bit_array.append(0)

    random.seed(553)
    usered_list = []
    answer = []
    for times in range(num_of_asks):
        stream_users = bx.ask(input_path, stream_size)
        list2 = []
        for user in stream_users:
            user = user.rstrip()
            list1 = myhash(user)
            index_list = []
            for i in list1:
                binary_number = bin(i)
                strip_string = binary_number.rstrip('0')
                number_zero = len(binary_number) - len(strip_string)
                index_list.append(number_zero)
            list2.append(index_list)
        #print(list2)
        a = 0
        list3 = []
        for i in range(10):
            list3.append(-1)
            for j in list2:
                if 2**j[i] > list3[i]:
                    list3[i] = 2**j[i]
        avg1 = int(sum(list3)/len(list3))
        answer.append((times, len(stream_users), avg1))
    a = 0
    b = 0
    for i in answer:
        a = a + i[1]
        b = b + i[2]
    avg = b/a

    header = ('Time', 'Ground Truth', 'Estimation')
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(answer)

    end_time = time.time()
    time = end_time - start_time
    print(time)
    print(avg)