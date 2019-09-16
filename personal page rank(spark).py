import findspark
findspark.init("/u/cs451/packages/spark")

from pyspark import SparkContext, SparkConf

import random
sc = SparkContext(appName="YourTest", master="local[2]", conf=SparkConf().set('spark.ui.port', random.randrange(4000,5000)))

def num_nodes_edges():
 #   """Returns a tuple (num_nodes, num_edges)"""
    num_nodes = 0
    num_edges = 0
    with open('p2p-Gnutella08-adj.txt') as f:
        t1 = sc.parallelize(f)
        num_nodes = t1.count()
        def helper(line):
            t = line.split()
            return len(t)-1
        t2=t1.map(helper)
        num_edges = t2.reduce(lambda x,y: x+y)
        

    return (num_nodes,num_edges)

        
    
    
def out_counts():
	 #   """Returns How many nodes of each outdegree are there"""
    with open('p2p-Gnutella08-adj.txt') as f:
        t1 = sc.parallelize(f)
        def helper(line):
            t = line.split()
            return (len(t)-1,1)
        t2 = t1.map(helper)
        t3 = t2.reduceByKey(lambda x,y: x+y)
        t4 = t3.sortByKey().collectAsMap()
        
    return t4


def in_counts():
	#   """return how many nodes of each indegree are there"""
    with open('p2p-Gnutella08-adj.txt') as f:
        t1 = sc.parallelize(f)
        def helper(line):
            t = line.split()
            if len(t) != 0:
                l = []
                for i in range(1,len(t)):
                    l.append((t[i],1))
            return l
        t2 = t1.flatMap(helper)
        t3 = t2.reduceByKey(lambda x,y: x+y)
        def helper2(tup):
            return(tup[1],1)
        t4 = t3.map(helper2)
        t5 = t4.reduceByKey(lambda x,y: x+y)
        t6 = t5.sortByKey().collectAsMap()
                
    return t6

 
#The following function should perform personalized page rank, with respect to the specified source node, 
    #over the Gnutella graph, for the specified number of iterations,
    # The output of your function should be a list of the 10 nodes with the highest personalized page rank with respect to the given source. For each of the 10 nodes, 
    #return the node's id and page rank value as a tuple. The list returned by the function should therefore look something like this: 
    #[(node_id_1, highest_pagerank_value), ..., (node_id_10, 10th_highest_pagerank_value)]


def personalized_page_rank(source_node_id, num_iterations, jump_factor):

    with open('p2p-Gnutella08-adj.txt') as f:
        t1 = sc.parallelize(f)
        def out(line):
            word = line.split()
            return (int(word[0]),word[1:len(word)])
        outter = t1.map(out)

    Pagebank = sc.parallelize([(source_node_id,1)])  
    def cal(line):
        l = line[1][1]
        result = []
        if len(l)!= 0:
            for i in l:
                result += [(int(i),(line[1][0]/len(l))*(1-jump_factor))]
            result += [(source_node_id,(line[1][0])*jump_factor)]
        else:
            result += [(source_node_id,line[1][0])]
        return result
    
    for i in range(0,num_iterations):
        r = Pagebank.join(outter)  
        re = r.flatMap(cal)
        Pagebank = re.reduceByKey(lambda x,y: x+y)
    
    result=Pagebank.top(10, key=lambda items: items[1])
           
    return result


 #sample output:
 #personalized_page_rank(0, 3, 0.5)
 #[(0, 0.5804166666666667),
 #(9, 0.03066820987654321),
 #(4, 0.03042013888888889),
 #(7, 0.03029513888888889),
 #(5, 0.03027932098765432),
 #(8, 0.03027777777777778),
 #(3, 0.03013888888888889),
 #(1, 0.03),
 #(2, 0.03),
 #(10, 0.03)]


#modify your personalized page rank implementation above so that it iterates until the maximum node change is less than  0.5𝑁 , where  𝑁  represents the number of nodes in the graph

def personalized_page_rank_stopping_criterion(source_node_id, jump_factor):
    with open('p2p-Gnutella08-adj.txt') as f:
        t1 = sc.parallelize(f)
        n = t1.count()
        def out(line):
            word = line.split()
            return (int(word[0]),word[1:len(word)])
        outter = t1.map(out)

    Pagebank = sc.parallelize([(source_node_id,1)])  
    def cal(line):
        l = line[1][1]
        result = []
        if len(l)!= 0:
            for i in l:
                result += [(int(i),(line[1][0]/len(l))*(1-jump_factor))]
            result += [(source_node_id,(line[1][0])*jump_factor)]
        else:
            result += [(source_node_id,line[1][0])]
        return result
    
    def caldiff(line):
        if line[1][1] == None:
            return line[1][0]
        if line[1][0] == None:
            return line[1][1]
        else:
            return abs(line[1][0] - line[1][1])
    
    maxdiff = 1
    while(maxdiff >= 0.5/n):
        init = Pagebank
        r = Pagebank.join(outter) 
        re = r.flatMap(cal)
        Pagebank = re.reduceByKey(lambda x,y: x+y)
        diff = Pagebank.leftOuterJoin(init)
        maxdiff = diff.map(caldiff).top(1)[0]

    

    result=Pagebank.top(10, key=lambda items: items[1])
           
    return result

   
