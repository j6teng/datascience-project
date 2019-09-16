# this imports the SimpleTokenize function from the simple_tokenize.py file that you uploaded

# the log function for computing PMI
# for the sake of consistency across solutions, please use log base 10
from math import log
import re

# this captures sequences of alphabetic characters
# possibily followed by an apostrophe and more characters
# n.b. (?:...) denotes a non-capturing group
def simple_tokenize(s):
    return re.findall(r"[a-z]+(?:'[a-z]+)?",s.lower())

###################################################################################################################
from collections import Counter
numline = 0
c1=Counter()
with open('Shakespeare.txt') as f:
    for line in f:
        numline+=1
        # tokenize, one line at a time
        t = simple_tokenize(line)
        c1.update(t)
        for k in range(len(t)):
                change = (t.count(t[k])-1)
                c1[t[k]] -= change

# Now, let's tokenize Shakespeare's plays
     
###################################################################################################################

###################################################################################################################
#  the user interface below defines the types of PMI queries that users can ask
#  you will need to modify it - where indicated - to access the results of your PMI analysis (above)
#  so that the queries can be answered
###################################################################################################################

while True:
    q = input("Input 1 or 2 space-separated tokens (return to quit): ")
    if len(q) == 0:
        break
    q_tokens = simple_tokenize(q)
          
    if len(q_tokens) == 1:
        threshold = 0
        while threshold <= 0:
            try:
                threshold = int(input("Input a positive integer frequency threshold: "))
            except ValueError:
                print("Threshold must be a positive integer!")
                continue
                
                
        paircount=Counter()
    ##    ccc=0
        with open('Shakespeare.txt') as f:
            for line in f:
                dup=Counter()
                ccc+=1
                t2 = simple_tokenize(line)
                if q_tokens[0] in t2:  
                    for word in t2:
                        if word != q_tokens[0]:
                            p1 = (word,q_tokens[0])
                            dup[p1]+=1
                            if dup[p1]<2:
                                paircount[p1]+=1
                     

        ##        if ccc>25:
        ##            break
              
     ##calculate pairs 
        maxfive=[]
        if len(paircount) >= 5:
            maxfive = paircount.most_common(5)
        else:
            maxfive = paircount.most_common(len(paircount))    
        v=0
      ##  print(maxfive)
        n1 = c1[q_tokens[0]]
        np1 = n1/numline
       
        # and output the result.
        # The print() statements below exist to show you the desired output format.
        # Replace them with your own output code, which should produce results in a similar format.
        print("  n({0}) = {1}".format(q_tokens[0],n1))
        print("  high PMI tokens with respect to {0} (threshold: {1}):".format(q_tokens[0],threshold))
        for pair in maxfive:
            word = pair[0]
          ##  print(pair)
            if(paircount[word] >= threshold):
                n2 = c1[word[0]]
                np2 = n2/numline
         ##       print(np2)
         ##       print(np1)
                n12 = pair[1]
                np12 = n12/numline
          ##      print(np12)
                npmi = np12/(np1*np2)
          ##      print(npmi)
                print("    n({0},{1}) = {2},  PMI({0},{1}) = {3}".format(q_tokens[0],word[0],n12,npmi))    
        
        # in the above, all XXX values should be at least as large as the threshold

    elif len(q_tokens) == 2:
        numpair = 0
  ##      cccc=0
        
        with open('Shakespeare.txt') as f:
            for line in f:
                cccc+=1
                t3 = simple_tokenize(line)
                if q_tokens[0] in t3:
                    if q_tokens[1] in t3:
                        numpair+=1
     ##           if cccc>25:
      ##              break

        
        int1 = c1[q_tokens[0]]
        int2 = c1[q_tokens[1]]
        inp2 = numpair
        p1 = int1/numline
  ##      print(p1)
        p2 = int2/numline
 ##       print(p2)
        p12 = inp2/numline
  ##      print(p12)
        pmi = p12/(p1*p2)
 ##       print(pmi)
        print("  n({0},{1}) = {2}".format(q_tokens[0],q_tokens[1],inp2))
        print("  PMI({0},{1}) = {2}".format(q_tokens[0],q_tokens[1],pmi))
    else:
        print("Input must consist of 1 or 2 space-separated tokens!")
