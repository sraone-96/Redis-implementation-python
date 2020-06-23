# Redis-implementation-python
Implemetation of few redis commands like GET, SET, ZADD, ZRANGE etc using python sortedLists
For any queries mail : saisravan.m@research.iiit.ac.in


<!---
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

## How to run
```
python3 redis.py
```
**NOTE** : We donot store the database we use for storing information at multiple intervals. Rather we store it when exiting or starting of the code to reduce data writing overhead eachtime. Hence use **Ctrl+C** or **Ctrl+D** to exit.

------!>
