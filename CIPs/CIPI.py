#CIP-I code on Spark

from __future__ import division
from pyspark import SparkContext,SparkConf,StorageLevel
from itertools import *
from operator import itemgetter
import time
import gc

def getKey(item):
    return item[3]

def updateSimilarity(block):
    for ele1 in block:
        for ele2 in block:
            if ele1[0]!=ele2[0]:
                epsilon= 1/ abs(block.index(ele1)-block.index(ele2))
                if ele1[1]<ele2[1]:
                    # add key (ele1[0],ele2[0]) with value 1+epsilon
                    item_sim_list.append(((ele1[0],ele2[0]),float(1+epsilon)))
                        
                else:
                    #add key (ele2[0],ele1[0]) with value 1+epsilon
                    item_sim_list.append(((ele2[0],ele1[0]),float(1+epsilon)))
def intersect(a, b):
     return list(set(a) & set(b))

def nearestNeighbors(item_id,simlist,k):
    '''
    Sort the predictions list by similarity and select the top-k neighbors
    '''
    neighbors=map(lambda x: x[0],sorted(simlist,key = itemgetter(1),reverse=True)[:k])
    return item_id, neighbors
    #items_and_sims.sort(key=lambda x: x[1][0],reverse=True)
    #return item_id, items_and_sims[:n]

def keyOnFirstItem(item_sim_data):
    '''
    For each item-item pair, make the first item's id the key
    '''
    (item1_id,item2_id,score) = item_sim_data
    return item1_id,(item2_id,score)



#Global variables
item_list=[]
item_sim_list=[]
item_similarity=dict()
item_network=dict()
blocktime=10 #in seconds to determine blocks
K=30 #number of neighbors
N=10 #number of recommendations

def main():
    global item_sim_list
    #lines = sc.textFile("u.data").map(lambda x: x.split("\t"))
    #lines = sc.textFile("data/ratings.dat").map(lambda x: x.split("::"))
    
    #ml-100k
    #lines = sc.textFile("ml-100k.data").map(lambda x: x.split("\t"))

    #ciao
    lines = sc.textFile("ciao_20.txt").map(lambda x: x.split("\t"))

    #epinion
    #lines = sc.textFile("epinion_100.txt").map(lambda x: x.split("\t"))


    #movietweetings: 450883 ratings
    #lines = sc.textFile("MovieTweetings.dat").map(lambda x: x.split("::"))

    #ml-1m
    #lines = sc.textFile("ml-1m.dat").map(lambda x: x.split("::"))

    #ml-10m
    #lines = sc.textFile("ml-10m.dat").map(lambda x: x.split("::"))

    #ml-20m
    #lines = sc.textFile("ml-20m.csv").map(lambda x: x.split(","))

    ratingEvent=lines.collect()
    
    ratingEvent.sort(key=getKey, reverse=False)
    
    i=0
    blockprofiles= dict()
    userprofiles= dict()
    itemsimRDD=sc.parallelize([])
    #start = time.time()
    #Training set
    while i<len(ratingEvent)-2000:
        if i%10000==0: print "INFO: ",i," ratings processed!" 
	'''
	if i%10000==0: 
            #itemsimRDD=sc.parallelize(item_sim_list).reduceByKey(lambda x,y:x+y,numPartitions=partitionCount)
	    itemsimRDD=sc.parallelize(item_sim_list).reduceByKey(lambda x,y:x+y).persist(StorageLevel.MEMORY_AND_DISK_SER)
            item_sim_list=itemsimRDD.collect()
            itemsimRDD.unpersist()
	    del itemsimRDD
	    gc.collect()
	'''
	if ratingEvent[i][1] not in item_list: item_list.append(ratingEvent[i][1])
        if ratingEvent[i][0] not in userprofiles:
            userprofiles[ratingEvent[i][0]]=[]
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        else:
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
            
        if ratingEvent[i][0] in blockprofiles:
            if len(blockprofiles[ratingEvent[i][0]])>1 and (long(ratingEvent[i][3])-long(blockprofiles[ratingEvent[i][0]][len(blockprofiles[ratingEvent[i][0]])-1][1])) > blocktime :
                updateSimilarity(blockprofiles[ratingEvent[i][0]])
                del blockprofiles[ratingEvent[i][0]][:]
		blockprofiles[ratingEvent[i][0]]=[]
            blockprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        else:
            blockprofiles[ratingEvent[i][0]]=[]
            blockprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        i=i+1


    start = time.time()
    '''
    itemsimRDD=sc.parallelize(item_sim_list).reduceByKey(lambda x,y:x+y)
    itemsimRDD1=itemsimRDD.map(lambda x: [int(x[0][0]),int(x[0][1]),float(x[1])]).sortBy(lambda x: x[0])
    
    itemtopKRDD=itemsimRDD1.map(lambda p: keyOnFirstItem(p)).groupByKey().map(lambda p: nearestNeighbors(p[0],list(p[1]),K))
    item_sims = itemtopKRDD.collect()
    '''
    #RDD for item-item similarities
    itemsimRDD=sc.parallelize(item_sim_list).reduceByKey(lambda x,y:x+y,numPartitions=partitionCount).persist(StorageLevel.DISK_ONLY)
    itemsimRDD1=itemsimRDD.map(lambda x: [int(x[0][0]),int(x[0][1]),float(x[1])]).sortBy(lambda x: x[0],numPartitions=partitionCount).persist(StorageLevel.DISK_ONLY)

    #RDD for top-k neighbors
    itemtopKRDD=itemsimRDD1.map(lambda p: keyOnFirstItem(p)).groupByKey(numPartitions=partitionCount).map(lambda p: nearestNeighbors(p[0],list(p[1]),K))
    item_sims = itemtopKRDD.collect()
	
    print "actual size: ",len(item_list)
    print "LEN: ",len(item_sims) 
    for (item,neighbors) in item_sims:
        item_network[item] = neighbors

    end = time.time()  
    
    print "users: ",len(userprofiles)

    itemsimRDD.unpersist()
    itemsimRDD1.unpersist()
    itemtopKRDD.unpersist()
    del itemsimRDD
    del itemsimRDD1
    del itemtopKRDD
    gc.collect()
    #set of items for each user based on their preference
    preferenceSet=dict()
    recommendationSet=dict()
    testprofiles=dict()
    del item_sim_list[:] 
    recoCount=0 
    #Test set
    while i<len(ratingEvent):
        if i%1000==0: print "INFO: ",i," test events completed!"
        if ratingEvent[i][1] not in item_list: item_list.append(ratingEvent[i][1])
        #new recommendation set
        preferenceSet[ratingEvent[i][0]]=dict()
        if ratingEvent[i][0] not in testprofiles:
            #preferenceSet[ratingEvent[i][0]]=dict()
            recommendationSet[ratingEvent[i][0]]=set([])
            testprofiles[ratingEvent[i][0]]=set([])
        
        if ratingEvent[i][0] in userprofiles:
            temp2=map(lambda x: int(x[0]),userprofiles[ratingEvent[i][0]])
            for item in userprofiles[ratingEvent[i][0]]:
                if int(item[0]) not in item_network.keys():
                    continue 
                for neighbor in item_network[int(item[0])]:
                    if neighbor not in temp2:
                        if neighbor not in preferenceSet[ratingEvent[i][0]]:
                            preferenceSet[ratingEvent[i][0]][neighbor]=1
                        else:
                            preferenceSet[ratingEvent[i][0]][neighbor]=preferenceSet[ratingEvent[i][0]][neighbor]+1
	    #print preferenceSet[ratingEvent[i][0]]
            temp1= map(lambda x: x[0],sorted(preferenceSet[ratingEvent[i][0]].items(), key=itemgetter(1),reverse=True))[:N]
            del preferenceSet[ratingEvent[i][0]]
	    recommendationSet[ratingEvent[i][0]].update(set(temp1))
	    recoCount=recoCount+1
	    #recommendationSet[ratingEvent[i][0]].update(set(list(set(temp1) - set(intersect(temp1, temp2)))))
            testprofiles[ratingEvent[i][0]].add(int(ratingEvent[i][1]))

	'''
        if ratingEvent[i][0] not in userprofiles:
            userprofiles[ratingEvent[i][0]]=[]
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        else:
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
	'''
        i=i+1
        
    prCount=0
    precisionSum=0
    recallSum=0
    norec=0
    for key,profile in testprofiles.iteritems():
        val=recommendationSet[key]
        if len(val)==0: 
            norec=norec+1
            continue
	prCount=prCount+1
        precisionSum=precisionSum+len(profile.intersection(val))/len(val)
        recallSum=recallSum+len(profile.intersection(val))/len(profile)
    Precision= precisionSum/ prCount
    Recall= recallSum/ prCount
    print "Precision: ", Precision, "Recall: ", Recall
    print "prCount: ", prCount
    print "norec: ", norec
    print "Total recCount: ",recoCount   
    file = open("output_precision_I2.txt", "w")
    file.write(str(Precision)+'\n')
    file.write(str(Recall)+'\n')
    file.write(str(end-start)+' seconds'+'\n')
    file.close()

if __name__ == "__main__":
    #conf = (SparkConf().setAppName("I2").set("spark.executor.memory", "200g").set("spark.driver.memory", "200g").set("spark.executor.cores","100").set("spark.driver.cores","100").set("spark.driver.maxResultSize","300g"))
    conf=(SparkConf().setAppName("I2").set("spark.driver.maxResultSize","500g"))
    sc = SparkContext(conf = conf)
    partitionCount=100
    main()
