# Redis-implementation-python
Implemetation of few redis commands like GET, SET, ZADD, ZRANGE etc using python sortedLists
For any queries mail : saisravan.m@research.iiit.ac.in


---------------------
### Basic Implementation details
-------------
Implementation is in python3. To optimize the code and runtime for queries we have used built in python *SortedContainers* library. From that we use SortedSet for our implementation.
Support for only the following commands from [REDIS](https://redis.io/commands) documentation: 
1. GET  
2. SET
3. EXPIRE
4. ZADD
5. ZRANGE
6. ZRANK


### Installation
--------------
##### Requirements
```
Python3
SortedConatainers
```

#### Installing SortedContainers:
```
pip3 install sortedcontainers
```
For more information on installation and implementation, please go through official [SortedContainer documnetation](http://www.grantjenks.com/docs/sortedcontainers/).

### Usage
------
Basic implementation without threadings.
```
python3 redis.py
```
Server-Client mode (1 server, multiple clients)
```
python3 server.py			#From terminal 1
python3 client.py			#open a new terminal for a new client.
							#Donot use the same terminal where server runs
```
**NOTE** : The database used for storing information is not saved at multiple intervals. Rather we store it when exiting or starting of the code to reduce data writing overhead eachtime. Hence use **Ctrl+C** or **Ctrl+D** to exit.

Sample Commands:
```
"GET firstkey" 
"SET firstkey firstvalue"
"GET firstkey"
"EXPIRE firstkey 5"
"GET firstkey"
"SET secondkey secondvalue EX 2"
"GET secondkey"
"SET secondkey thirdvalue NX"
"GET secondkey"
"ZADD myzset 2 two 3 three"
"ZRANGE myzset 0 -1 WITHSCORES"
"ZRANK myzset three"
"ZRANGE myzset 0 1 WITHSCORES"
"ZADD thirdkey 1 one "
"ZADD thirdkey XX 4 one"
"ZADD thirdkey NX 5 one"
"ZADD thirdkey NX 6 one"
"ZADD thirdkey NX 6 two"
"ZADD thirdkey XX CH INCR 4 two"
"ZADD thirdkey NX CH INCR 4 two"
```
---------
