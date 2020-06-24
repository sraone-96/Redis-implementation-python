import os
import json
import pickle
import threading
import sys
import socket
from _thread import *
import threading 
from sortedcontainers import SortedList, SortedSet, SortedDict 

##################################################################

# Code is an implementation of few redis ommands like SET, GET ZADD, ZRANK etc; Look for all funtions
# in "execute" method
# To run      :   python3 redis.py
# To close    :   (ctrl +c)  OR (ctrl+d)

# This code takes in commands with arguments in "redis syntax". please use the same syntax

# Brief explanation of datastructures and methods used: Simple "Set" in python is not sorted, and uses
# a hashmap to store values. Hence, we have used "SortedSet" from "SortedContainers" library. Please
# install "SortedContainers". Explanation of each method is commented in the given method itself.

# Memory is persitant ie; even after closing, the memory in the sets is stored in a json file in the
# same directory. This memory file is loaded into main memory upon executing this python code.  

###################################################################

class Redis(object):

    # Class to define a redis element

    # M                       :       Map to store simple SET and GET variables list
    # Ttl
    # sorted_set
    # scores_map
    # storage_file
    # set_storage_file

    def __init__(self, storage_file="storage.pickle"):
        self.data = {"map": {}, "set": {}, "ttl": {}}
        self.storage_file = storage_file
        self.lock = threading.Lock() 

        # To check if storage file already exists, and load the file. Otherwise write a new file
        # to store the datastructures     

        if os.path.exists(self.storage_file):
            with open(self.storage_file, "rb") as handle:
                self.data = pickle.load(handle)
        else:            
            with open(self.storage_file, 'wb') as handle:
                pickle.dump(self.data, handle, protocol=pickle.HIGHEST_PROTOCOL)


    def redis_set(self, args):

        # Function for SET command in redis. 
        # args FORMAT   :   key value [EX seconds|PX milliseconds] [NX|XX] [KEEPTTL]

        if len(args) < 2:
            return ("Key and Value Expected")
        final_ans = 0
        op = 1
        print(final_ans)

        if "NX" in args:
            try:
                temp = self.data["map"][args[0]]
                return "nil"
            except KeyError:
                self.data["map"][args[0]] = args[1]
                final_ans = 1

        elif "XX" in args:
            try:
                temp = self.data["map"][args[0]]
                self.data["map"][args[0]] = args[1]
                final_ans = 1
            except KeyError:
                return "nil"

        else:
            self.data["map"][args[0]] = args[1]
            final_ans = 1

        print(final_ans,op)
            
        if "EX" in args:
            idx = args.index("EX")
            try:
                op = self.redis_expire([args[0], args[idx+1]])
            except IndexError:
                return "Insufficient Arguments"

        elif "PX" in args:
            idx = args.index("PX")
            try:
                op = self.redis_expire([args[0], args[idx+1]])
            except IndexError:
                return "Insufficient Arguments"
        
        if(final_ans==1 and op==1):
            return "OK"
        else:
            return "nil"

    def redis_get(self, args):
        
        # Function for GET command in redis. 
        # args FORMAT   :   key

        if len(args) == 0:
            return "Key Expected"

        try:
            return self.data["map"][args[0]]
        except KeyError:
            return 'nil'


    def redis_expire(self, args):

        # Function for EXPIRE command in redis. 
        # args FORMAT   :   key

        if len(args) < 2:
            return "Key and Timeout Expected"
        
        if args[0] in self.data["map"].keys():
            try:
                self.data["ttl"][args[0]].cancel()
            except KeyError:
                pass
            self.data["ttl"][args[0]] = threading.Timer(float(args[1]), self.delete_key, args=[args[0]])
            self.data["ttl"][args[0]].start()
            return 1
        else:
            return 0
            
    def redis_zadd(self, args, ch=False, incr=False):
       
        # Function for ZADD command in redis. 
        # args FORMAT   :   key [NX|XX] [CH] [INCR] score member  

        if len(args) < 3:
            return "Set with score and value expected"

        ans = 0         # Final output based on changes. Such as 0 incase no update, 
                        # x incase of x new insertions. The new score incase of Increment command
        st = 1
        if ("CH" in args):
        	ch=True
        	st += 1
        if("INCR" in args):
        	incr=True
        	st += 1

        if ("XX" in args) and ("NX" in args):
            return "Error : only XX or NX can be performed not both together"

        # creating new key incase already not existing
        if not args[0] in self.data["set"].keys():
            self.data["set"][args[0]] = {"sorted_set": SortedSet(), "scores_map": {}}

        if "XX" in args:
            
            # update elements that already exist. dont add new ones.

            for i in range(st+1, len(args)-1, 2):
                if args[i+1] in self.data["set"][args[0]]["scores_map"]:

                    # removing the entry first. and adding again
                    # Whether to update score of existing entry or add an existing entry again. 
                    # This is an easier way. 

                    # remove
                    self.data["set"][args[0]]["sorted_set"].remove((self.data["set"][args[0]]["scores_map"][args[i+1]], args[i+1]))
                    # add with new score
                    self.add_element(args[0], args[i], args[i+1], incr)
                    
                    if ch:                              # If change is True, consider updated ones too
                        ans += 1                
                    if incr:
                        ans = self.data["set"][args[0]]["scores_map"][args[i+1]]    # new score of the 
                                                                                    # incremented element    
                else:                   
                    if incr:
                        return "nil"
                
        elif "NX" in args:
            
            # create new element. Dont change anything if exists already.
            
            for i in range(st+1, len(args)-1, 2):
                if args[i+1] in self.data["set"][args[0]]["scores_map"]:
                    if incr:
                        return "nil"
                    continue            # if element already exists donot change anything
                else:
                    self.add_element(args[0], args[i], args[i+1], False)
                
                ans += 1
                if incr:
                    ans = args[i]

        else:
            for i in range(st, len(args)-1, 2):
                if args[i+1] in self.data["set"][args[0]]["scores_map"]:
                
                    # removing the entry first. and adding again
                    # Whether to update score of existing entry or add an existing entry again. 
                    # This is an easier way. 
                    
                    # remove
                    self.data["set"][args[0]]["sorted_set"].remove((self.data["set"][args[0]]["scores_map"][args[i+1]], args[i+1]))
                    # adding element
                    self.add_element(args[0], args[i], args[i+1], incr)
                    
                    if ch:
                        ans += 1
                    
                    if incr:
                        ans = self.data["set"][args[0]]["scores_map"][args[i+1]]
                else:
                    self.add_element(args[0], args[i], args[i+1], False)
                    ans += 1
                    if incr:
                        ans = args[i]
        return ans

    def add_element(self, set, score, new_value, incr):
        score = float(score)                # converting score to float
        
        if not incr:
            # adding element to the sortedset
            self.data["set"][set]["sorted_set"].add((score, new_value))
            self.data["set"][set]["scores_map"][new_value] = score

        elif incr:
            try:    
                # update new_score = old_score + increment_value
                self.data["set"][set]["sorted_set"].add((score + self.data["set"][set]["scores_map"][new_value], new_value))
                
                # print(score + self.data["set"][set]["scores_map"][new_value])
                self.data["set"][set]["scores_map"][new_value] = score + self.data["set"][set]["scores_map"][new_value]
            
            except KeyError:
                # Incase key doesnot exist, just add it to the sorted set as new element
                self.data["set"][set]["sorted_set"].add((score, new_value))
                self.data["set"][set]["scores_map"][new_value] = score
                


    def delete_key(self, key):
        del self.data["map"][key]
        del self.data["ttl"][key]    

    def redis_zrank(self, args):

        # Function for ZRANK command in redis. 
        # args FORMAT   :   key member  


        if len(args) < 2:
            return "Set with score and value expected"

        if not args[-1] in self.data["set"][args[0]]["scores_map"]:
            return "nil"

        return self.data["set"][args[0]]["sorted_set"].index((self.data["set"][args[0]]["scores_map"][args[-1]], args[-1]))

    def redis_zrange(self, args):


        # Function for ZRANK command in redis. 
        # args FORMAT   :   ZRANGE key start stop [WITHSCORES]

        if len(args) < 3:
            return "Set with range expected"

        if int(args[1]) < 0:
            args[1] = self.data["set"][args[0]]["sorted_set"].__len__() + int(args[1])
        if int(args[2]) < 0:
            args[2] = self.data["set"][args[0]]["sorted_set"].__len__() + int(args[2]) + 1
        if "WITHSCORES" in args:
            return self.data["set"][args[0]]["sorted_set"][int(args[1]):int(args[2])+1]
        else:
            return [value for index, value in self.data["set"][args[0]]["sorted_set"][int(args[1]):int(args[2])+1]]

    def execute(self, query):
        # Function to execute all the possible commands.
        # query : string with the redis syntax.

        args = query.split()    # args is list of strings. For "GET mykey", args = ["GET","mykey"]

        if args[0] == "GET":
            return self.redis_get(args[1:])

        elif args[0] == "SET":
            self.lock.acquire()
            ANS = self.redis_set(args[1:])
            self.lock.release()
            return ANS

        elif args[0] == "EXPIRE":
            self.lock.acquire()
            ANS = self.redis_expire(args[1:])
            self.lock.release()
            return str(ANS)

        elif args[0] == "ZADD":
            self.lock.acquire()
            ANS = self.redis_zadd(args[1:])
            self.lock.release()
            return str(ANS)

        elif args[0] == "ZRANK":
            return str(self.redis_zrank(args[1:]))

        elif args[0] == "ZRANGE":
            return str(self.redis_zrange(args[1:]))

        elif args[0] == "SAVE" or args[0] == "EXIT":
            self.save_to_file()

        else:
            return "unknown command"


    def save_to_file(self):
        # Funciton to save the current state of information in all the data structures into
        # a single file 
        with open(self.storage_file, 'wb') as handle:
              pickle.dump(self.data, handle, protocol=pickle.HIGHEST_PROTOCOL)

def threaded(c, r): 
    while True: 
        # data received from client
        data = c.recv(1024)

        temp = r.execute(data.decode("ascii")) 
        print(temp)
        if not data or data.decode("ascii") == "EXIT": 
            print('Bye') 
            break

        # send back reversed string to client 
        # temp = "result"
        c.send(temp.encode('ascii')) 

    # connection closed 
    c.close() 

if __name__ == '__main__': 

    host = "127.0.0.1" 

    # reverse a port on your computer 
    # in our case it is 12345 but it 
    # can be anything 

    port = 12345
    r = Redis()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s.bind((host, port)) 
    print("socket binded to port", port) 

    # put the socket into listening mode 
    s.listen(5) 
    print("socket is listening") 

    # a forever loop until client wants to exit 
    while True: 

        # establish connection with client 
        c, addr = s.accept() 

        print('Connected to :', addr[0], ':', addr[1]) 

        # Start a new thread and return its identifier 
        start_new_thread(threaded, (c, r)) 
    s.close()     
