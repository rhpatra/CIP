from __future__ import division
from itertools import *
from operator import itemgetter
import numpy as np

item_similarity=dict()
item_network=dict()
item_neighbors=dict()
item_list=[]
pu=dict()
qi=dict()
bu=dict()
bi=dict()
training=[]
mu=0
maxRating=5
mucount=0
gamma=0.05
lambdaval=0.1
num_fac=100 #number of factors
N=5 #number of recommendations

def r_bar(u,i):
    val=mu
    if u in bu:
        val=val+bu[u]
    if i in bi:
        val=val+bi[i]
    #print pu[u]
    if u in pu and i in qi:
	#print qi[i]
        val=val+np.vdot(pu[u],qi[i])
    #print val    
    if val>maxRating:
        return maxRating
    elif val<1:
        return 1
    else:
        return val


def getKey(item):
    return item[3]

def intersect(a, b):
    return list(set(a) & set(b))

def getItemKey(item):
    print "item: ",item[0]
    return item[0]

def main():
    ratingEvent=[]
    i=0
    
    #ml-100k,ciao,epinion
    #with open('ciao_20.txt', 'r') as f:    
    with open('ml-100k.data', 'r') as f:
        for line in f:
            ratingEvent.append(line.split('\t'))
            #print ratingEvent[i][3]
            i=i+1
    
    #ml-10m
    ''' 
    with open('ml-10m.dat', 'r') as f:
        for line in f:
            ratingEvent.append(line.split('::'))
            #print ratingEvent[i][3]
            i=i+1
    '''

    '''
    #ml-1m
    with open('ml-1m.dat', 'r') as f:
        for line in f:
            ratingEvent.append(line.split('::'))
            #print ratingEvent[i][3]
            i=i+1
    '''

    '''
    #ml-20m
    with open('ml-20m.csv', 'r') as f:
        for line in f:
            ratingEvent.append(line.split(','))
            #print ratingEvent[i][3]
            i=i+1
    '''


    ratingEvent.sort(key=getKey, reverse=False)
    
    i=0
    userprofiles= dict()
    #Training set
    while i<len(ratingEvent)-2000:
        if i%10000==0: print "INFO: ",i," ratings processed!"
	global mucount,mu
        mucount=mucount+1
        mu=mu+float(ratingEvent[i][2])
        if ratingEvent[i][1] not in item_list: item_list.append(ratingEvent[i][1])
        if ratingEvent[i][0] not in userprofiles:
            userprofiles[ratingEvent[i][0]]=[]
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        else:
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        
        training.append((ratingEvent[i][0],ratingEvent[i][1],ratingEvent[i][2]))
        
        if ratingEvent[i][0] not in pu:
            pu[ratingEvent[i][0]]=np.random.rand(1,num_fac)
            bu[ratingEvent[i][0]]=np.random.rand(1,1)
        
        if ratingEvent[i][1] not in qi:
            qi[ratingEvent[i][1]]=np.random.rand(1,num_fac)
            bi[ratingEvent[i][1]]=np.random.rand(1,1)
        
        #print training
        i=i+1
    mu= mu/mucount
    l=0
    while l<50:
        l=l+1
        for item in training:
            error=(float(item[2])-r_bar(item[0],item[1]))
            bu[item[0]]=bu[item[0]]+gamma*(error - lambdaval*bu[item[0]])
            bi[item[1]]=bi[item[1]]+gamma*(error - lambdaval*bi[item[1]])
            qi[item[1]]=np.add(qi[item[1]],np.multiply(gamma,np.add(np.multiply(error, pu[item[0]]),np.multiply(-lambdaval,qi[item[1]]))))
            pu[item[0]]=np.add(pu[item[0]],np.multiply(gamma,np.add(np.multiply(error, qi[item[1]]),np.multiply(-lambdaval,pu[item[0]]))))
   
        print "INFO: Epoch ",l," complete!"  

    #update factors
    
    
    
    
    
    #set of items for each user based on their preference
    MAECount=0
    MAESum=0
    recommendationSet=dict()
    testprofiles=dict()
    trainingprofiles=userprofiles.copy() 
    #Test set
    while i<len(ratingEvent):
        if i%1000==0: print "INFO: ",i," test events completed!" 
        #if ratingEvent[i][1] not in item_list: item_list.append(ratingEvent[i][1])
        #new recommendation set
        MAECount=MAECount+1
        MAESum=MAESum+abs(r_bar(ratingEvent[i][0],ratingEvent[i][1])-float(ratingEvent[i][2]))
	#print "MAE: ",MAESum/MAECount
	#if ratingEvent[i][0] not in preferenceSet:
        preferenceSet=dict()
        if ratingEvent[i][0] not in testprofiles:
	    recommendationSet[ratingEvent[i][0]]=set([])
            testprofiles[ratingEvent[i][0]]=set([])
        if ratingEvent[i][0] in trainingprofiles:
            temp2=map(lambda x: x[0],userprofiles[ratingEvent[i][0]])
            for item in item_list:
                if item not in temp2:
                    preferenceSet[item]=r_bar(ratingEvent[i][0],item)
            #print "here: ",preferenceSet[ratingEvent[i][0]][item]
            #print "here"
            
            temp1= map(lambda x: x[0],sorted(preferenceSet.items(), key=itemgetter(1),reverse=True))[:N]
	    #print temp1
	    preferenceSet.clear()            
            #print "temp1: ",temp1
            #print "temp2: ",temp2
            #print intersect(temp1, temp2)
            #['50', '181', '257', '100', '237', '222', '151', '117', '118', '121']
            recommendationSet[ratingEvent[i][0]].update(set(temp1))
	    #recommendationSet[ratingEvent[i][0]].update(set(list(set(temp1) - set(intersect(temp1, temp2)))))
            #print recommendationSet[ratingEvent[i][0]]
	    testprofiles[ratingEvent[i][0]].add(ratingEvent[i][1])
        #print ratingEvent[i][0]
        #print recommendationSet[ratingEvent[i][0]]
        if ratingEvent[i][0] not in userprofiles:
            userprofiles[ratingEvent[i][0]]=[]
        userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        #else:
        #    userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        
        #print "len: ",len(testprofiles)
        #print "profile: ",testprofiles
        i=i+1
    
    prCount=0
    precisionSum=0
    recalSum=0
    MAE=MAESum/ MAECount
    print "MAE: ",MAE
    for key,profile in testprofiles.iteritems():
        #print "key: ",key
        #print "p[0]: ",profile
        val=recommendationSet[key]
        if len(val)==0: continue
        prCount=prCount+1
	#print "profile len: ",len(profile)
        #print "reco len: ",len(val)
        #print "intersection: ",len(profile.intersection(val))
        precisionSum=precisionSum+len(profile.intersection(val))/len(val)
        recalSum=recalSum+len(profile.intersection(val))/len(profile)
    
    Precision= precisionSum/ prCount
    Recall= recalSum/ prCount
    print "Precision: ",Precision
    print "Recall: ", Recall
    print "Reco set size: ",N

    file = open("output_precision_svd.txt", "w")
    file.write(str(Precision)+'\n')
    file.write(str(Recall)+'\n')
    file.close()


if __name__ == "__main__":
    main()
