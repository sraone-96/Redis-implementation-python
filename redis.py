import os
import json
import pickle
import threading
import sys
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
            print("Key and Value Expected")
            return

        if "NX" in args:
            try:
                temp = self.data["map"][args[0]]
            except KeyError:
                self.data["map"][args[0]] = args[1]

        elif "XX" in args:
            try:
                temp = self.data["map"][args[0]]
                self.data["map"][args[0]] = args[1]
            except KeyError:
                pass

        else:
            self.data["map"][args[0]] = args[1]
            
        if "EX" in args:
            idx = args.index("EX")
            try:
                self.redis_expire([args[0], args[idx+1]])
            except IndexError:
                print("Insufficient Arguments")

        elif "PX" in args:
            idx = args.index("PX")
            try:
                self.redis_expire([args[0], args[idx+1]])
            except IndexError:
                print("Insufficient Arguments")


    def redis_get(self, args):
        
        # Function for GET command in redis. 
        # args FORMAT   :   key

        if len(args) == 0:
            print("Key Expected")
            return

        try:
            print(self.data["map"][args[0]])
        except KeyError:
            print('nil')



    def redis_expire(self, args):

        # Function for EXPIRE command in redis. 
        # args FORMAT   :   key

        if len(args) < 2:
            print("Key and Timeout Expected")
            return
        
        if args[0] in self.data["map"].keys():
            try:
                self.data["ttl"][args[0]].cancel()
            except KeyError:
                pass
            self.data["ttl"][args[0]] = threading.Timer(float(args[1]), self.delete_key, args=[args[0]])
            self.data["ttl"][args[0]].start()
        else:
            print("Key not found")
            return
            
    
    def redis_zadd(self, args, ch=False, incr=False):
       
        # Function for ZADD command in redis. 
        # args FORMAT   :   key [NX|XX] [CH] [INCR] score member  

        if len(args) < 3:
            print("Set with score and value expected")

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
            raise Exception("Error : only single value can be incremented")

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
            print("Set with score and value expected")

        if not args[-1] in self.data["set"][args[0]]["scores_map"]:
            return "nil"

        return self.data["set"][args[0]]["sorted_set"].index((self.data["set"][args[0]]["scores_map"][args[-1]], args[-1]))

    def redis_zrange(self, args):


        # Function for ZRANK command in redis. 
        # args FORMAT   :   ZRANGE key start stop [WITHSCORES]

        if len(args) < 3:
            print("Set with range expected")

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
            self.redis_get(args[1:])

        elif args[0] == "SET":
            self.redis_set(args[1:])

        elif args[0] == "EXPIRE":
            self.redis_expire(args[1:])

        elif args[0] == "ZADD":
            print(self.redis_zadd(args[1:]))

        elif args[0] == "ZRANK":
            print(self.redis_zrank(args[1:]))

        elif args[0] == "ZRANGE":
            print(self.redis_zrange(args[1:]))

        else:
            print("unknown command")


    def save_to_file(self):
        # Funciton to save the current state of information in all the data structures into
        # a single file 
        with open(self.storage_file, 'wb') as handle:
              pickle.dump(self.data, handle, protocol=pickle.HIGHEST_PROTOCOL)



if __name__ == "__main__":
    
    import time

    r = Redis()
    while(1):
        try:
            command = input()    
            r.execute(command)

        except (KeyboardInterrupt,EOFError):
            r.save_to_file()
            sys.exit(0)

    # examples commands for testing
    # r = Redis()
    # r.execute("GET firstkey")
    # r.execute("SET firstkey firstvalue")
    # r.execute("GET firstkey")
    # r.execute("EXPIRE firstkey 5")
    # time.sleep(5)
    # r.execute("GET firstkey")
    # r.execute("SET secondkey secondvalue")
    # r.execute("SET secondkey thirdvalue NX")
    # r.execute("GET secondkey")
    # r.execute("ZADD myzset 1 one")
    # r.execute("ZADD myzset 2 two 3 three")
    # r.execute("ZRANGE myzset 0 -1 WITHSCORES")
    # r.execute("ZRANK myzset three")
    # r.execute("ZRANGE myzset 0 1 WITHSCORES")
    # print("-"*30)
    # r.execute("ZADD thirdkey 1 one")
    # with open(r.storage_file, 'wb') as handle:
    # 	pickle.dump(r.data, handle, protocol=pickle.HIGHEST_PROTOCOL)
    # r.execute("ZADD thirdkey XX 4 one")
    # r.execute("ZADD thirdkey NX 5 one")
    # r.execute("ZADD thirdkey NX 6 one")
    # r.execute("ZADD thirdkey NX 6 two")
    # r.execute("ZADD thirdkey XX CH INCR 4 \"two\" ")
    # r.execute("ZADD thirdkey NX CH INCR 4 two")
    # r.execute("ZADD thirdkey CH INCR 4 three")
    # r.execute("ZRANGE thirdkey 0 -1 WITHSCORES")
    # # print(r.data["set"]["thirdkey"]["scores_map"])
    # with open(r.storage_file, 'wb') as handle:
    # 	pickle.dump(r.data, handle, protocol=pickle.HIGHEST_PROTOCOL)