
import multiprocessing
from deepdist import DeepDist
from gensim.models.word2vec import Word2Vec
from pyspark import SparkContext

def generate_vocab(fname):
    count=0
    print "generating vocab..."
    text=[]
    with open(fname) as f:
        for line in f:
            parts=line.split('\t')
            if len(parts)<2:
                print "here"
                continue
            content=parts[1].rstrip().split(',')
            del content[-1]
            text.append(" ".join(content))

    return text

#ml-100k
#fname='/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-100k/user_contents_sessions_1hr_ml100k.txt'

#ml-1m
fname='/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-1m/user_contents_sessions_1hr_ml1m.txt'

#ciao
#fname='/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ciao/user_contents_sessions_1hr_ciao.txt'


transactions= generate_vocab(fname)
corpus = sc.parallelize(transactions).map(lambda s: s.split())
                
                
def gradient(model, sentences):  # executes on workers
    syn0, syn1 = model.syn0, model.syn1
    model.train(sentences)
    return {'syn0': model.syn0 - syn0, 'syn1': model.syn1 - syn1}

def descent(model, update):      # executes on master
    model.syn0 += update['syn0']
    model.syn1 += update['syn1']


basemodel=Word2Vec(seed=339, size=200, window=3, min_count=1, sample=0, negative=4, iter=1)
basemodel.build_vocab(corpus.collect())

basemodel.syn1=0
    
print "vocab built!"
    
with DeepDist(basemodel,min_updates=10) as dd:
    dd.train(corpus, gradient, descent)
    dd.model.init_sims(replace=True)
    #Save the model so that it can be loaded and trained later with new batches of CIPs
    dd.model.save_word2vec_format('w2v_CIP_neg4_i1_w3_sessions_1hr_win3_mc1_size200.model.bin',binary=True)
    
print "Complete!"


from collections import OrderedDict

def prepare_recommendations(fname,w2v_model):
    max_similar_items= 100
    item_list=[]
    max_reco_size=5
    userprofiles= OrderedDict()
    train_userprofiles= OrderedDict()
    lines = sc.textFile(fname).map(lambda x: x.split("\t"))
    ratingEvent=lines.collect()
    
    i=0
    
    while i<len(ratingEvent)-20000:
        i=i+1
        if i%100000==0: 
            print "INFO: ",i," ratings processed!"
            
        if ratingEvent[i][1] not in item_list: 
            item_list.append(ratingEvent[i][1])
            
        if ratingEvent[i][0] not in userprofiles:
            userprofiles[ratingEvent[i][0]]=[]
            train_userprofiles[ratingEvent[i][0]]=[]
        
        userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
        train_userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])
     
    
    testprofiles=dict()
    recommendationSet=dict()
    recoCount=0
    while i<len(ratingEvent)-1:
        
        
        recommendations=[]
        i=i+1
        if i%1000==0: print "INFO: ",i," test events completed!"
        
        if ratingEvent[i][1] not in item_list: item_list.append(ratingEvent[i][1])
            
            
        if ratingEvent[i][0] not in testprofiles:
            recommendationSet[ratingEvent[i][0]]=set([])
            testprofiles[ratingEvent[i][0]]=set([])
            
        if ratingEvent[i][0] in userprofiles:
            
            
            train_user_all_items=map(lambda x: str(x[0]),train_userprofiles[ratingEvent[i][0]])
            
            user_recent_items=map(lambda x: str(x[0]),userprofiles[ratingEvent[i][0]])[-3:]
            
            #print user_recent_items
            #print d2v_model.most_similar(positive=user_recent_items, topn=max_similar_items)
            intial_count=20
            candidates = [x[0].encode('ascii','ignore') for x in w2v_model.most_similar(positive=user_recent_items, topn=max_similar_items, restrict_vocab=intial_count)]
            while len(candidates)<60:
                candidates = [x[0].encode('ascii','ignore') for x in w2v_model.most_similar(positive=user_recent_items, topn=max_similar_items, restrict_vocab=intial_count)]
                intial_count=intial_count+10
                
                
            recCount=0 
            
            for reco in candidates:
                if reco in train_user_all_items:
                    continue
                    

                recCount=recCount+1
                
                if recCount>max_reco_size:
                    recoCount=recoCount+1
                    break
                    
                recommendations.append(reco)
             
            
            recommendationSet[ratingEvent[i][0]].update(set(recommendations))
            testprofiles[ratingEvent[i][0]].add(str(ratingEvent[i][1]))
            #print recommendationSet[ratingEvent[i][0]]
            #update only for users in training set
            userprofiles[ratingEvent[i][0]].append([ratingEvent[i][1],ratingEvent[i][3]])

            
        
    prCount=0
    precisionSum=0
    recallSum=0
    norec=0
    print "test: ",len(testprofiles)
    for key,profile in testprofiles.iteritems():
        val=recommendationSet[key]
        #print len(val), " , ",len(profile)
        if len(val)==0: 
            norec=norec+1
            continue
        prCount=prCount+1
        precisionSum=precisionSum + len(profile.intersection(val))/(len(val)+0.0)
        recallSum=recallSum + len(profile.intersection(val))/(len(profile)+0.0)
        
        #print "intersection: ",profile.intersection(val)
        
        #print "precisionSum: ",precisionSum, " recallSum: ",recallSum
        

    Precision= precisionSum/ (prCount+0.0)
    Recall= recallSum/ (prCount+0.0)
    print "Precision: ", Precision, "Recall: ", Recall
    print "prCount: ", prCount
    print "norec: ", norec
    print "Total recCount: ",recoCount   
        
    
    
from gensim import models
w2v_model= models.Word2Vec.load_word2vec_format("/Users/rhicheekpatra/Desktop/Gensim_Experiments/codes/w2v_CIP_neg4_i1_w3_sessions_1hr_win3_mc1_size200.model.bin",binary=True)

#ml-1m
prepare_recommendations("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-1m/ratings_tsv_sorted.dat",w2v_model)


#ml-100k
#prepare_recommendations("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-100k/u_sorted.data",w2v_model)


#ciao
#prepare_recommendations("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ciao/ciao_20_sorted.txt",w2v_model)
