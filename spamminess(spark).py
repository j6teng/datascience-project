import findspark, random
findspark.init("/u/cs451/packages/spark")
from spamminess import spamminess
from math import exp
from pyspark import SparkContext, SparkConf
import shutil, os
from random import seed
from random import random

sc = SparkContext(appName="YourTest", master="local[2]", conf=SparkConf().set('spark.ui.port', random.randrange(4000,5000)))


def sequential_SGD(model, training_dataset='/u/cs451/public_html/spam/spam.train.group_x.txt', delta = 0.002):
    #### Your solution to Question 1 here
    # open one of the training files - defaults to group_x
    with open(training_dataset) as f:
        for line in f:
            F = line.split(" ")
            t = F[1]
            F.remove(F[0])
            F.remove(F[0])
            score = spamminess(F,model)
            prob = 1.0/(1+exp(-score))
            for f in F:
                if t == 'spam':
                    if model.get(f) == None:
                        model[f] = (1.0-prob)*delta
                    else:
                        model[f] += (1.0-prob)*delta
                if t == 'ham':
                    if model.get(f) == None:
                        model[f] = -prob*delta
                    else:
                        model[f] -= prob*delta
    return model
                    
    #      each line represents a document
    #      read and parse the line
    #      Let:
    #        t represent the spam/ham tag for this document
    #        F represent the list of features for this document

    #      find the spamminess of the current document using the current model:
    #      score = spamminess(F,w)

    #      then, update the model:
    #      prob = 1.0/(1+exp(-score))
    #      for each feature f in F:
    #          if t == 'spam':
    #              increase model(f) by (1.0-prob)*delta (or set model(f) to (1.0-prob)*delta if f is not in the dict yet)
    #          elif t == 'ham':
    #              decrease model(f) by prob*delta (or set model(f) to -prob*delta if f is not in the dict yet)



def spark_SGD(training_dataset='/u/cs451/public_html/spam/spam.train.group_x.txt', output_model='models/group_x_model', delta = 0.002):
    if os.path.isdir(output_model):
        shutil.rmtree(output_model) # Remove the previous model to create a new one
   # training_data = sc.textFile(training_dataset)
   # with open('testttt.txt') as f:
    t1 = sc.textFile(training_dataset)
    model = {}
    def helper(line):
        l = line.split(" ")
        docid = l[0]
        s = l[1]
        l.remove(l[0])
        l.remove(l[0])
        return (0,(docid,s,l))
        
    def helper2(line):
        training = line[1]
        for t in training:
            s= t[1]
            features = t[2]
            score = spamminess(features,model)
            prob = 1.0/(1+exp(-score))
            for f in features:
                if s == 'spam':
                    if model.get(f) == None:
                        model[f] = (1.0-prob)*delta
                    else:
                        model[f] += (1.0-prob)*delta
                if s == 'ham':
                    if model.get(f) == None:
                        model[f] = 0-prob*delta
                    else:
                        model[f] -= prob*delta
        return model

    t2 = t1.map(helper).groupByKey(1)
    t3 = t2.map(helper2)
    def helper3(line):
        l = [(k, v) for k, v in line.items()] 
        return l
    t4 = t3.flatMap(helper3)
    t4.saveAsTextFile(output_model)

#sampletest:
spark_SGD()
spark_SGD(training_dataset='/u/cs451/public_html/spam/spam.train.group_y.txt', output_model='models/group_y_model')
spark_SGD(training_dataset='/u/cs451/public_html/spam/spam.train.britney.txt', output_model='models/britney_model')



def spark_shuffled_SGD(training_dataset='/u/cs451/public_html/spam/spam.train.group_x.txt', output_model='models/group_x_model', delta = 0.002):
    if os.path.isdir(output_model):
        shutil.rmtree(output_model) # Remove the previous model to create a new one
    #training_data = sc.textFile(training_dataset)
    t1 = sc.textFile(training_dataset)
    model = {}
    def ram(line):
        seed(1)
        num = random()
        return(num,line)
    def ram2(line):
        return line[1]

    def helper(line):
        l = line.split(" ")
        docid = l[0]
        s = l[1]
        l.remove(l[0])
        l.remove(l[0])
        return (0,(docid,s,l))
        
    def helper2(line):
        training = line[1]
        for t in training:
            s= t[1]
            features = t[2]
            score = spamminess(features,model)
            prob = 1.0/(1+exp(-score))
            for f in features:
                if s == 'spam':
                    if model.get(f) == None:
                        model[f] = (1.0-prob)*delta
                    else:
                        model[f] += (1.0-prob)*delta
                if s == 'ham':
                    if model.get(f) == None:
                        model[f] = -prob*delta
                    else:
                        model[f] -= prob*delta
        return model
    t1 = t1.map(ram).sortByKey().map(ram2)
    t2 = t1.map(helper).groupByKey(1)
    t3 = t2.map(helper2)
    def helper3(line):
        l = [(k, v) for k, v in line.items()] 
        return l
    t4 = t3.flatMap(helper3)
    t4.saveAsTextFile(output_model)
    

    # sample test
spark_shuffled_SGD(output_model='models/group_x_model_shuffled')

def spark_classify(input_model='models/group_x_model', test_dataset='/u/cs451/public_html/spam/spam.test.qrels.txt', results_path='results/test_qrels'):
    if os.path.isdir(results_path):
        shutil.rmtree(results_path) # Remove the previous results
    #test_data = sc.textFile(test_dataset)
    model = sc.textFile(input_model+'/part-00000')
    def builddic(line):
        t = line[1:-1].split(",")
        f = t[0][1:-1]
        w = float(t[1])
        return (f,w)
    model = model.map(builddic).collectAsMap()
    t1 = sc.textFile(test_dataset)
    def helper(line):
        l = line.split(" ")
        docid = l[0]
        s = l[1]
        l.remove(l[0])
        l.remove(l[0])
        score = spamminess(l,model)
        if score>0:
            pred = 'spam'
        else:
            pred = 'ham'
        return (docid,s,score,pred)
    
    t2 = t1.map(helper)
    t2.saveAsTextFile(results_path)

#  Run the evaluation program like this, after first replacing "output-file"
#  with the name of the folder that holds your classifier's output
#  Note the "!" character, which is important.   This is the escape character
#  that tells the notebook to run an external program.
!/u/cs451/bin/spam_eval.sh results/test_qrels
#result : 1-ROCA%: 17.26


