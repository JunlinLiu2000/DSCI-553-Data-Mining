import sys
import time
import binascii
from operator import add
from blackbox import BlackBox
import random
import csv

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
        count1 = set()
        count2 = 0
        for user in stream_users:
            user = user.rstrip()
            list1 = myhash(user)
            for i in list1:
                if filter_bit_array[i] == 0:
                    filter_bit_array[i] == 1
                else:
                    if user not in usered_list:
                        count1.add(user)
            if user not in usered_list:
                count2 = count2 + 1
            usered_list.append(user)


    header = ('Time', 'FPR')
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(answer)
    #print(answer)

    end_time = time.time()
    time = end_time - start_time
    print(f"Duration:", time)


















