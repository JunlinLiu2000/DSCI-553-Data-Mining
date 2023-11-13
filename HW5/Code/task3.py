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

    random.seed(553)
    bx = BlackBox()
    answer = []
    index_list = [0, 20, 40, 60, 80]
    stream_users = bx.ask(input_path, stream_size)
    #print(stream_users)
    reservoir = []
    for user in stream_users:
        reservoir.append(user)

    num = len(stream_users)
    list1 = [num]
    for i in index_list:
        user = stream_users[i]
        list1.append(user)

    answer.append(list1)
    #print(stream_users)

    n = len(stream_users)
    s = len(stream_users)

    for times in range(num_of_asks-1):
        stream_users = bx.ask(input_path, stream_size)
        num = num + len(stream_users)
        list1 = [num]
        for user in stream_users:
            n = n + 1
            ran_num = random.random()
            if ran_num < float(s/n):
                ran_int = random.randint(0, 99)
                reservoir[ran_int] = user
        for i in index_list:
            u = reservoir[i]
            list1.append(u)
        answer.append(list1)
    #print(answer)
    header = ('seqnum', '0_id', '20_id', '40_id', '60_id', '80_id')
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(answer)
    #print(answer)

    end_time = time.time()
    time = end_time - start_time
    print(time)



