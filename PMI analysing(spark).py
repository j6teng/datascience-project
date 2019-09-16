import findspark
findspark.init("/u/cs451/packages/spark")
from math import log
from pyspark import SparkContext, SparkConf
import random
from collections import Counter
import re

# this captures sequences of alphabetic characters
# possibily followed by an apostrophe and more characters
# n.b. (?:...) denotes a non-capturing group
def simple_tokenize(s):
    return re.findall(r"[a-z]+(?:'[a-z]+)?",s.lower())

sc = SparkContext(appName="YourTest", master="local[2]", conf=SparkConf().set('spark.ui.port', random.randrange(4000,5000)))


# Returns the count of distinct tokens in the `Shakespeare.txt` dataset 
def count_distinct_tokens():  
    with open('Shakespeare.txt') as f:
        t1 = sc.parallelize(f)
        def helper(line):
            t = simple_tokenize(line)
            s = []
            for word in t:
                s.insert(0,(word,1))
            return s 
        r2 = t1.flatMap(helper)
        r3 = r2.reduceByKey(lambda a,b:a+b) 
        return r3.count()



# Returns the count of distinct pairs in the `Shakespeare.txt` dataset
def count_distinct_pairs():
    with open('Shakespeare.txt') as f:
        t1 = sc.parallelize(f)
        def helper(line):
            t = simple_tokenize(line)
            l = []
            for i in range(len(t)):
                for j in range(len(t)):
                    if t[i] != t[j]:
                        l.insert(0,((t[i],t[j]),1))
            return l 
        t2 = t1.flatMap(helper)
        t3 = t2.reduceByKey(lambda x,y: x+y)
        return t3.count()


# Returns a list of the top 50 (probability, count, token) tuples, ordered by probability
def top_50_tokens_probabilities():
    with open('Shakespeare.txt') as f:
        t1 = sc.parallelize(f)
        num = t1.count()
        if num<50:
            print("not enough token")
            return
        def helper(line):
            t = simple_tokenize(line)
            s = []
            for word in t:
                if (word,1) not in s:
                    s.insert(0,(word,1))
            return s 
        t2=t1.flatMap(helper)
        t3=t2.reduceByKey(lambda a,b:a+b) 
        def helper2(line):
            prob = line[1]/num
            return (prob,line[1],line[0])
        t4=t3.map(helper2)
        t5=t4.top(50, key=lambda items: items[0])
        return t5 


def PMI(threshold):
    with open('Shakespeare.txt') as f:
        t1 = sc.parallelize(f)
        num = t1.count()
        def calword(line):
            t = simple_tokenize(line)
            s = []
            for word in t:
                if (word,1) not in s:
                    s.insert(0,(word,1))
            return s 
        w2=t1.flatMap(calword)
        w3=w2.reduceByKey(lambda a,b:a+b) 
        
        def calpair(line):
            t = simple_tokenize(line)
            l = []
            for i in range(len(t)):
                for j in range(len(t)):
                    if t[i] != t[j]:
                        if ((t[i],t[j]),1) not in l:
                            l.insert(0,((t[i],t[j]),1))
            return l 
        p2=t1.flatMap(calpair)
        p3=p2.reduceByKey(lambda a,b:a+b) 
        def join1(line):
            return (line[0][0],(line[0],line[1]))
        j1=p3.map(join1).join(w3)       
        def join2(line):
            return(line[1][0][0][1],(line[1][0],(line[0],line[1][1])))
        j2=j1.map(join2).join(w3)
        def pmical(line):
            count1 = line[1][0][1][1]
            p1 = count1/num
            count2 = line[1][1]
            p2 = count2/num
            pair =line[1][0][0][0]
            countp = line[1][0][0][1]
            pp = countp/num
            pmi = log(pp/(p1*p2),10)
            return(pair,pmi,countp,count1,count2)
        total = j2.map(pmical)
        result = total.filter(lambda a:a[2]>=threshold)
        return result.collect()

#The function PMI should return a list of ((token1, token2), pmi, co-occurrence_count, token1_count, token2_count) tuples, 
#that is, the list returned by the function should be of the form: 
#[((token1, token2), pmi, cooc_count, token1_count, token2_count), (...), 
#((other_token1, other_token2), other_pmi, other_cooc_count, other_token1_count, other_token2_count)].
