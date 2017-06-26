#CIP-U code on Spark

from __future__ import division
from pyspark import SparkContext,SparkConf
from itertools import *
from operator import itemgetter
import time

#returns tuples which satisfies the condition
def par_updateSimilarity(u,v):
    P=dict()
    P_l=dict()
    D=dict()
    P[u]=map(lambda x:x[0],userprofiles[u])
    if u in deltaprofiles:
        #new updates
        D[u]=map(lambda x:x[0],deltaprofiles[u])
    else:
        #no new updates
        D[u]=[]
    #old profile
    P_l[u]=difference(P[u],D[u])
    
    P[v]=map(lambda x:x[0],userprofiles[v])
    if v in deltaprofiles:
        D[v]=map(lambda x:x[0],deltaprofiles[v])
    else:
        D[v]=[]
    P_l[v]=difference(P[v],D[v])
    
    C_uv=intersect(P_l[u],P_l[v])
    D_uv=union(union(intersect(P_l[u],D[v]),intersect(P_l[v],D[u])),intersect(D[u],D[v]))
    count=0
    for i in union(C_uv,D_uv):
        for j in D_uv:
            if i<j and abs(P[u].index(i)-P[u].index(j))<delta_H and abs(P[v].index(i)-P[v].index(j))<delta_H:
                count=count+1

    return (u,v),count

def getKey(item):
    return item[3]

def intersect(a, b):
    return list(set(a) & set(b))
    
def union(a,b):
    return list(set(a)|set(b))

def difference(a,b):
    return list(set(a)-set(b))

def getItemKey(item):
    #print "item: ",item[0]
    return item[0]


def nearestNeighbors(item_id,simlist,k):
    '''
    Sort the predictions list by similarity and select the top-k neighbors
    '''
    neighbors=map(lambda x: x[0],sorted(simlist,key = itemgetter(1),reverse=True)[:k])
    return item_id, neighbors

def keyOnFirstItem(item_sim_data):
    '''
    For each item-item pair, make the first item's id the key
    '''
    (item1_id,item2_id,score) = item_sim_data
    return item1_id,(item2_id,score)

def main():
    #ml-100k
    #lines = sc.textFile("ml-100k.data").map(lambda x: x.split("\t"))
    
    #ciao
    #lines = sc.textFile("ciao_20.txt").map(lambda x: x.split("\t"))

    #epinion
    #lines = sc.textFile("epinion_100.txt").map(lambda x: x.split("\t"))

    #ml-1m
    lines = sc.textFile("ml-1m.dat").map(lambda x: x.split("::"))

    #movietweetings: 450883 ratings
    #lines = sc.textFile("MovieTweetings.dat").map(lambda x: x.split("::"))

    #ml-10m
    #lines = sc.textFile("ml-10m.dat").map(lambda x: x.split("::"))

    #ml-20m
    #lines = sc.textFile("ml-20m.csv").map(lambda x: x.split(","))

    ratingEvent=lines.collect()

    ratingEvent.sort(key=getKey, reverse=False)        

    i=0
    #Training set
    while i<len(ratingEvent)-20000:
	if i%10000==0: print "INFO: ",i," ratings processed!"
        if ratingEvent[i][1] not in item_list: item_list.append(ratingEvent[i][1])
        #new user
        if ratingEvent[i][0] not in userprofiles:
            userItemCount[ratingEvent[i][0]]=0
            userprofiles[ratingEvent[i][0]]=[]
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
            deltaprofiles[ratingEvent[i][0]]=[]
            deltaprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
        else:
            userItemCount[ratingEvent[i][0]]=userItemCount[ratingEvent[i][0]]+1
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
            if ratingEvent[i][0] not in deltaprofiles:
                deltaprofiles[ratingEvent[i][0]]=[]
            deltaprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
        i=i+1
        
    start= time.time()    
    
    sc.broadcast(userprofiles)
    sc.broadcast(deltaprofiles)
   
    
    usersRDD=sc.parallelize(userprofiles).repartition(sqrtpartitionCount)
    combinationsRDD=usersRDD.cartesian(usersRDD).filter(lambda p: p[0]!=p[1])
    
    usersimRDD=combinationsRDD.map(lambda p: par_updateSimilarity(p[0],p[1])).filter(lambda p: p[1]>=0).reduceByKey(lambda x,y:x+y,numPartitions=partitionCount)
    
    tempSimList=usersimRDD.collect()

    usersimRDD1=usersimRDD.map(lambda x: [int(x[0][0]),int(x[0][1]),int(x[1])]).sortBy(lambda x: x[0],numPartitions=partitionCount)
    user_sims=usersimRDD1.map(lambda p: keyOnFirstItem(p)).groupByKey(numPartitions=partitionCount).map(lambda p: nearestNeighbors(p[0],list(p[1]),K)).collect()
    
    '''
    usersRDD=sc.parallelize(userprofiles)
    combinationsRDD=usersRDD.cartesian(usersRDD).filter(lambda p: p[0]!=p[1])

    usersimRDD=combinationsRDD.map(lambda p: par_updateSimilarity(p[0],p[1])).filter(lambda p: p[1]>0).reduceByKey(lambda x,y:x+y)

    tempSimList=usersimRDD.collect()

    usersimRDD1=usersimRDD.map(lambda x: [int(x[0][0]),int(x[0][1]),int(x[1])]).sortBy(lambda x: x[0])
    user_sims=usersimRDD1.map(lambda p: keyOnFirstItem(p)).groupByKey().map(lambda p: nearestNeighbors(p[0],list(p[1]),K)).collect()
    '''
   
    print "LEN: ",len(user_sims)
    print "actual len: ",len(userprofiles)

 
    for (user,neighbors) in user_sims:
        #print "user-id: ",user," value: ",neighbors
        user_network[user] = neighbors

    deltaprofiles.clear()
    end=time.time()
    
    usersRDD.unpersist()
    combinationsRDD.unpersist()
    usersimRDD.unpersist()
    usersimRDD1.unpersist()
    
    del user_sims[:]
    del user_sim_list[:]
    del usersRDD
    del combinationsRDD
    del usersimRDD
    del usersimRDD1
 
    #set of items for each user based on their preference
    preferenceSet=dict()
    recommendationSet=dict()
    testprofiles=dict()
     
    badprofileslist=[] 
 
    print "user_net size: ",len(user_network)
    #print "actual users: ",len(deltaprofiles)
    nu_Count=0
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
        #-------------
        if int(ratingEvent[i][0]) in user_network:
            temp2=map(lambda x: int(x[0]),userprofiles[ratingEvent[i][0]])
            for user in user_network[int(ratingEvent[i][0])]:
                temp3=map(lambda x: int(x[0]),userprofiles[str(user)])
                for itemn in temp3:
                    #print "neighbor: ",neighbor
                    #print "uid: ",user," user profiles: ",temp3
                    
                    if itemn not in temp2:
                        #print "Not in"
                        if itemn not in preferenceSet[ratingEvent[i][0]]:
                            preferenceSet[ratingEvent[i][0]][itemn]=1
                        else:
                            preferenceSet[ratingEvent[i][0]][itemn]=preferenceSet[ratingEvent[i][0]][itemn]+1
            #to be distributed
            temp1= map(lambda x: x[0],sorted(preferenceSet[ratingEvent[i][0]].items(), key=itemgetter(1),reverse=True))[:N]
            del preferenceSet[ratingEvent[i][0]]
            recommendationSet[ratingEvent[i][0]].update(set(temp1))
	    recoCount=recoCount+1
	    #recommendationSet[ratingEvent[i][0]].update(set(list(set(temp1) - set(intersect(temp1, temp2)))))
            testprofiles[ratingEvent[i][0]].add(int(ratingEvent[i][1]))
        else:
	    if ratingEvent[i][0] not in badprofileslist: badprofileslist.append(ratingEvent[i][0])
            nu_Count=nu_Count+1
        #-------------
        if ratingEvent[i][0] not in userprofiles:
            userItemCount[ratingEvent[i][0]]=0
            userprofiles[ratingEvent[i][0]]=[]
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
            deltaprofiles[ratingEvent[i][0]]=[]
            deltaprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
            
        else:
            userItemCount[ratingEvent[i][0]]=userItemCount[ratingEvent[i][0]]+1
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])
            if ratingEvent[i][0] not in deltaprofiles:
                deltaprofiles[ratingEvent[i][0]]=[]
            deltaprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3],userItemCount[ratingEvent[i][0]]])

            
            
        #INCREMENTAL UPDATE
        if i%incrCount==0:
            start= time.time()    
            sc.broadcast(userprofiles)
            sc.broadcast(deltaprofiles)
      
	    '''  
            usersRDD=sc.parallelize(userprofiles)
            combinationsRDD=usersRDD.cartesian(usersRDD).filter(lambda p: p[0]!=p[1])
    
            usersimRDD= sc.parallelize(tempSimList).union(combinationsRDD.map(lambda p: par_updateSimilarity(p[0],p[1]))).filter(lambda p: p[1]>0).reduceByKey(lambda x,y:x+y)
            tempSimList=usersimRDD.collect()
            usersimRDD1=usersimRDD.map(lambda x: [int(x[0][0]),int(x[0][1]),int(x[1])]).sortBy(lambda x: x[0])
            user_sims=usersimRDD1.map(lambda p: keyOnFirstItem(p)).groupByKey().map(lambda p: nearestNeighbors(p[0],list(p[1]),K)).collect()
    	    '''
	    
            usersRDD=sc.parallelize(userprofiles).repartition(sqrtpartitionCount)
            combinationsRDD=usersRDD.cartesian(usersRDD).filter(lambda p: p[0]!=p[1])

            usersimRDD= sc.parallelize(tempSimList).union(combinationsRDD.map(lambda p: par_updateSimilarity(p[0],p[1]))).filter(lambda p: p[1]>0).reduceByKey(lambda x,y:x+y,numPartitions=partitionCount)
            tempSimList=usersimRDD.collect()
            usersimRDD1=usersimRDD.map(lambda x: [int(x[0][0]),int(x[0][1]),int(x[1])]).sortBy(lambda x: x[0],numPartitions=partitionCount)
            user_sims=usersimRDD1.map(lambda p: keyOnFirstItem(p)).groupByKey(numPartitions=partitionCount).map(lambda p: nearestNeighbors(p[0],list(p[1]),K)).collect()
	   
    
            for (user,neighbors) in user_sims:
                #print "user-id: ",user," value: ",neighbors
                user_network[user] = neighbors
        
            end=time.time()
        
            print "update time: ",str(end-start)," seconds"
            
            usersRDD.unpersist()
            combinationsRDD.unpersist()
            usersimRDD.unpersist()
            usersimRDD1.unpersist()
            del usersRDD
            del combinationsRDD
            del usersimRDD
            del usersimRDD1
    
            deltaprofiles.clear()
            #print "len: ",len(testprofiles)
            #print "profile: ",testprofiles

        
        i=i+1
    
    print "nu_Count: ", nu_Count
    prCount=0
    precisionSum=0
    recallSum=0
    tmp=0
    for key,profile in testprofiles.iteritems():
        #print "key: ",key
        #print "p[0]: ",profile
        val=recommendationSet[key]
        if len(val)==0: 
            tmp=tmp+1
            continue
        prCount=prCount+1
        precisionSum=precisionSum+len(profile.intersection(val))/len(val)
        recallSum=recallSum+len(profile.intersection(val))/len(profile)
        
    Precision= precisionSum/ prCount
    Recall= recallSum/ prCount
    
    print "Precision: ",Precision, "Recall: ",Recall
    print prCount
    print tmp
    print "final users: ",len(userprofiles)
    print "Reco: ",recoCount

    print "bad profiles: ",len(badprofileslist)
    file = open("output_precision_IKNN.txt", "w")
    file.write(str(Precision)+'\n')
    file.write(str(Recall)+'\n')
    file.write(str(end-start)+' seconds'+'\n')
    file.close()

if __name__ == "__main__":
    conf = (SparkConf().setAppName("IKNN").set("spark.executor.memory", "150g").set("spark.driver.memory", "150g").set("spark.executor.cores","100").set("spark.driver.cores","100").set("spark.shuffle.compress","true").set("spark.shuffle.spill.compress","true").set("spark.rdd.compress","true").set("spark.driver.maxResultSize","50g"))
    sc = SparkContext(conf = conf)
    user_sim_list=[]
    userprofiles= dict()
    deltaprofiles= dict()
    user_similarity=dict()
    user_network=dict()
    user_neighbors=dict()
    userItemCount=dict()
    item_list=[]
    partitionCount=100
    sqrtpartitionCount=10
    incrCount=10000000 #number of ratings after which incremental update done
    K=200 #number of neighbors
    N=10 #number of recommendations
    delta_H=40 #threshold
    main()
