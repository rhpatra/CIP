package org.ts;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;








public class JaccardImplicitRecommender {
	//public static LinkedHashMap<Integer, ArrayList<Integer>> RatingOrder=new LinkedHashMap<Integer, ArrayList<Integer>>();
	public static ArrayList<Integer> userList=new ArrayList<Integer>();
	public static ArrayList<Integer> itemList=new ArrayList<Integer>();
	public static ArrayList<Integer> timelineUser = new ArrayList<Integer>();
	public static ArrayList<Integer> timelineItem = new ArrayList<Integer>();	
	public static ArrayList<Double> timelineRating = new ArrayList<Double>();
	public static ArrayList<Long> timelineTime = new ArrayList<Long>();
	//	public static LinkedHashMap<Integer, LinkedHashMap<Integer,Long>> userTimeProfiles = new LinkedHashMap<Integer, LinkedHashMap<Integer,Long>>();
	public static Hashtable<Integer, ScoreCount<Integer>> wholeProfiles = new Hashtable<Integer, ScoreCount<Integer>>();
	public static Hashtable<Integer, ScoreCount<Integer>> userProfiles = new Hashtable<Integer, ScoreCount<Integer>>();
	public static LinkedHashMap<Integer, LinkedHashMap<Integer,Long>> trainingProfiles = new LinkedHashMap<Integer, LinkedHashMap<Integer,Long>>();
	public static Hashtable<Integer, ScoreCount<Integer>> testProfiles = new Hashtable<Integer, ScoreCount<Integer>>();
	public static int[][] UserCount = new int[1683][1683];
	public static int[] FracCount=new int[10];
	public static Hashtable<Integer, ScoreCount<Integer>> network=new Hashtable<Integer, ScoreCount<Integer>>();
	public static int NEIGHBORHOOD_SIZE,NB_RECO;
	public static double Fraction;
	public static Random random = new Random();
	public static Hashtable<Integer, ArrayList<Integer>> receivedItemsList = new Hashtable<Integer, ArrayList<Integer>>();
	public static Hashtable<Integer, ArrayList<Integer>> recommendedItems = new Hashtable<Integer, ArrayList<Integer>>();
	public static LinkedHashMap<Integer,ArrayList<Integer>> receivedItems=new LinkedHashMap<Integer,ArrayList<Integer>>();
	public static Hashtable<Integer, ArrayList<Double>> ratings = new Hashtable<Integer, ArrayList<Double>>();
	public static Hashtable<Integer, ArrayList<Double>> ratingsItem = new Hashtable<Integer, ArrayList<Double>>();
	public static int TASK_NUM, DATASET;
	public static ConcurrentHashMap<Integer, Integer> CommonItemsDist=new ConcurrentHashMap<Integer, Integer>();
	public static long tmin;
	public static long tmax;
	public static long tborne;
	public static double[][] ratingScheme=new double[5][5];
	public static Hashtable<Integer, Long> itemLaunchTime = new Hashtable<Integer, Long>();
	//public static int TASK_NUM=2;
	public static void main(String[] args) throws Exception  {


		//Parameters
		//Argument 1
		NEIGHBORHOOD_SIZE= 50;
		//NEIGHBORHOOD_SIZE=Integer.parseInt(args[0]);

		//Argument 2
		NB_RECO=10;
		//NB_RECO=Integer.parseInt(args[1]);

		//Argument 4
		TASK_NUM=1;
		//TASK_NUM=Runtime.getRuntime().availableProcessors();
		//TASK_NUM=Integer.parseInt(args[3]);

		//Argument 5
		//DATASET= 1 for ml100k, 2 for Ciao and 3 for Ml1m
		//DATASET=Integer.parseInt(args[4]);
		DATASET=1;



		int nb=0;


		if(DATASET==1){
			//Movilens 100k dataset
			initTimeLine();
		}else if(DATASET==2){
			//Ciao Dataset
			initTimeLineCiao();
		}else if(DATASET==3){
			//Movielens 1M dataset
			initTimeLine1M();
		}


		//Size of Profile
		//CommonThres=itemList.size();
		//CommonThres=(int) Math.sqrt(itemList.size());
		//CommonThres=(int) Math.pow((Math.log(itemList.size())/Math.log(2)), 1);

		LinkedHashMap<Long, ArrayList<Rating>> t = new LinkedHashMap<Long, ArrayList<Rating>>();


		for(int i=0;i<5;i++){
			//item launch group-> i
			//user purchase group -> j
			for(int j=0;j<5;j++){
				ratingScheme[i][j]= j+ 0.2*(i+1);
			}
		}

		System.out.println("Rating Matrix:");
		long[] array = new long[timelineTime.size()];
		for(int i=0; i<timelineTime.size(); i++) {

			long l = timelineTime.get(i);
			array[i]=l;
			if(!t.containsKey(l)) {
				t.put(l, new ArrayList<Rating>());
			}
			t.get(l).add(new Rating(timelineUser.get(i), timelineRating.get(i), timelineItem.get(i)));
		}

		ArrayList<Long> timelineTime2 = new ArrayList<Long>(t.keySet());
		Collections.sort(timelineTime2);

		Arrays.sort(array);
		
		//System.out.println("Array: "+Arrays.toString(array));

		// subtract number of test samples
		tborne = array[timelineTime.size()-2000];
		
		//System.out.println("Time Counts: "+(int)(timelineTime.size()-2000));
		System.out.println("Minimum Timestamp: "+timelineTime2.get(0));
		System.out.println("Minimum Timestamp: "+timelineTime2.get(timelineTime2.size()-1));



		//Number of days to consider for phase transition between items
		int numDays=30;

		int trainingCount=0,testCount=0,recoCount=0;


		//Test time: tborne

		boolean doKNN=true;
		for(Long l : timelineTime2) {

			for(Rating rate : t.get(l)) {
				nb++;
				if(tborne>l){
					//System.out.println("Below");
					double pseudoRating;
					int i,j;
					if(itemLaunchTime.get(rate.getRid())==null){
						itemLaunchTime.put(rate.getRid(), l);
					}


					int itemPeriod= (int) ((tborne-itemLaunchTime.get(rate.getRid()))/(double)86400);
					int userPeriod= (int) ((tborne-l)/(double)86400);


					if(itemPeriod/(double)numDays>4) i=4;
					else i= (int) (itemPeriod/(double)numDays);

					if(userPeriod/(double)numDays>4) j=4;
					else j= (int) (userPeriod/(double)numDays);

					//System.out.println("i,j= "+i+","+j);

					pseudoRating=ratingScheme[4-i][4-j];
					//System.out.println("Time difference: "+ (l-itemLaunchTime.get(rate.getRid())));
					//item purchase time: l
					//item launch time: itemLaunchTime.get(rate.getRid())


					trainingProfiles.get(rate.getUid()).put(rate.getRid(), l);
					userProfiles.get(rate.getUid()).addValue(rate.getRid(), pseudoRating);
					trainingCount++;

				}else{
					
					if(doKNN){
						//Fast KNN
						//parallelKNN(userList);
						//System.out.println("Hello");
						long startTime = System.currentTimeMillis();
						int iter_Limit=userList.size()/TASK_NUM;
						KNNThread[] threads=new KNNThread[TASK_NUM];
						for(int i=0;i<TASK_NUM-1;i++){
							threads[i]= new KNNThread(userList.subList(i*iter_Limit,(i+1)*iter_Limit));
						}
						threads[TASK_NUM-1]= new KNNThread(userList.subList((TASK_NUM-1)*iter_Limit,userList.size()));
						for(int i=0;i<TASK_NUM;i++){
							threads[i].start();
						}
						for(int i=0;i<TASK_NUM;i++){
							threads[i].join();
						}

						System.out.println("Total KNN runtime: "+(System.currentTimeMillis()-startTime));
						doKNN=false;
					}
					
					//System.out.println("Above");
					int uid=rate.getUid();

					if(testProfiles.get(uid)==null){
						testProfiles.put(uid, new ScoreCount<Integer>());
						recommendedItems.put(uid, new ArrayList<Integer>());
					}

					if(trainingProfiles.get(uid).size()>0){
						HashMap<Integer,Integer> items=new HashMap<Integer,Integer>();
						JaccardImplicitRecommender x=new JaccardImplicitRecommender();
						ValueComparator bvc = x.new ValueComparator(items);
						TreeMap<Integer,Integer> itemsSorted = new TreeMap<Integer,Integer>(bvc);

						for(int nnid: network.get(uid).getItems()){
							for(int id:trainingProfiles.get(nnid).keySet()){

								if(userProfiles.get(uid).contains(id)) continue;

								if(items.get(id)==null){
									items.put(id, 1);
								}
								else{
									Integer tmp=items.get(id);
									items.remove(id);
									items.put(id, tmp + 1);
								}
							}
						}
						//System.out.println("Items in user profile: "+trainingProfiles.get(uid).size());
						if(items.size()==0) System.out.println("Problem");
						//if(testProfiles.get(uid).size()==0) System.out.println("Uid: "+uid+"Test Profile size problem!!");
						itemsSorted.putAll(items);

						//System.out.println("user id: "+rate.getUid()+"Sorted items: "+itemsSorted);
						List<Integer> recommendations=new ArrayList<Integer>();
						//int count=testProfiles.get(uid).size()*NB_RECO;
						recommendations=putFirstEntries(NB_RECO,itemsSorted);
						//System.out.println("item id: "+rate.getUid()+"recommended items: "+recommendations);
						//System.out.println("Actual item: "+rate.getRid());
						recommendedItems.get(uid).addAll(recommendations);
						recoCount++;

					}
					testProfiles.get(uid).addValue(rate.getRid(),rate.getRate());
					testCount++;

					userProfiles.get(rate.getUid()).addValue(rate.getRid(), rate.getRate());

				}
			}
		}

		//Compute precision +recall
		double Precision=0;
		double Recall=0;
		double measureCount=0;


		for(int uid: testProfiles.keySet()){

			if(trainingProfiles.get(uid).size()>0){	
				List ProfileItems = testProfiles.get(uid).getItems();

				ProfileItems.retainAll(recommendedItems.get(uid));

				if(recommendedItems.get(uid).size()==0)
					System.out.println("Problem!!");

				measureCount++;
				Precision+=ProfileItems.size()/(double)recommendedItems.get(uid).size();
				Recall+= ProfileItems.size()/(double)testProfiles.get(uid).getItems().size();
			}
		}


		System.out.println("Number of neighbors: "+NEIGHBORHOOD_SIZE);
		System.out.println("Precision: "+(Precision/(double)measureCount));
		System.out.println("Recall: "+(Recall/(double)measureCount));
		System.out.println("Test ratings: "+testCount);
		System.out.println("Training ratings: "+trainingCount);
		System.out.println("Reco count: "+recoCount);

	}


	public static void parallelKNN(List<Integer> userList){
		ScoreCount<Integer> friends=new ScoreCount<Integer>();
		ArrayList<Integer> friends_2=new ArrayList<Integer>();

		int userCount=0;
		for(int gid:userList){
			//System.out.println("user id: "+gid);
			userCount++;
			//Cosine based Approach
			friends_2=Util.getClosestNeighbor(NEIGHBORHOOD_SIZE, userProfiles.get(gid), userProfiles,gid);
			//System.out.println("Neighbors: "+friends_2);
			network.put(gid, new ScoreCount<Integer>());
			//network.get(gid).clear();
			for(int fid:friends_2){
				network.get(gid).addValue(fid,1);
			}
		}
	}

	public static class KNNThread extends Thread {
		private final List<Integer> userList;
		KNNThread(List<Integer> List) {
			this.userList=List;
		}

		public void run() {
			parallelKNN(userList);
		}
	}
	public static List<Integer> pickNRandom(Set<Integer> lst, int n) {
		List<Integer> copy = new LinkedList<Integer>(lst);
		Collections.shuffle(copy);
		if(lst.size()<n) return copy;

		return copy.subList(0, n);
	}

	public static ArrayList<Integer> putFirstEntries(int max, TreeMap<Integer, Integer> source) {
		//List<Integer> list=new ArrayList<Integer>();
		int count = 0;
		TreeMap<Integer, Integer> target = new TreeMap<Integer, Integer>();
		for (Map.Entry<Integer, Integer> entry:source.entrySet()) {
			if (count >= max) break;

			target.put(entry.getKey(), entry.getValue());
			count++;
		}
		ArrayList list = new ArrayList<>(target.keySet());
		return list;
	}
	public class ValueComparator implements Comparator<Integer> {

		HashMap<Integer, Integer> base;
		public ValueComparator(HashMap<Integer, Integer> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with equals.    
		public int compare(Integer a, Integer b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			} // returning 0 would merge keys
		}
	}

	public static List<Integer> merge(List<Integer> l1, List<Integer> l2) {
		for (int index1 = 0, index2 = 0; index2 < l2.size(); index1++) {
			if (index1 == l1.size() || l1.get(index1) > l2.get(index2)) {
				l1.add(index1, l2.get(index2++));
			}
		}

		return l1;
	} 


	/*
	 * load the 1m dataset
	 * 
	 */

	private static void initTimeLine1M(){

		try {
			BufferedReader br = new BufferedReader(new FileReader("dataset/ml-1m/ratings.dat"));

			String line;
			while ((line = br.readLine()) != null) {
				Integer uid = Integer.parseInt(line.substring(0, line.indexOf("::")));
				if(!userList.contains(uid)) {
					userList.add(uid);
					wholeProfiles.put(uid, new ScoreCount<Integer>());
					userProfiles.put(uid, new ScoreCount<Integer>());
					trainingProfiles.put(uid, new LinkedHashMap<Integer,Long>());
					receivedItemsList.put(uid, new ArrayList<Integer>());
					receivedItems.put(uid, new ArrayList<Integer>());
					ratings.put(uid, new ArrayList<Double>());
				}
				timelineUser.add(uid);
				line = line.substring(line.indexOf("::")+2);
				Integer rid = Integer.parseInt(line.substring(0, line.indexOf("::")));
				timelineItem.add(rid);
				if(!itemList.contains(rid)){
					ratingsItem.put(rid, new ArrayList<Double>());
					itemList.add(rid);
				}

				line = line.substring(line.indexOf("::")+2);
				Double rating = Double.parseDouble(line.substring(0, line.indexOf("::")));
				timelineRating.add(rating);
				ratings.get(uid).add(rating);
				ratingsItem.get(rid).add(rating);
				wholeProfiles.get(uid).addValue(rid,rating);

				line = line.substring(line.indexOf("::")+2);
				long time = Long.parseLong(line);
				timelineTime.add(time);
			}
			br.close();


		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}




	private static void initTimeLineAmazon(){
		try {
			//BufferedReader br = new BufferedReader(new FileReader("/Users/rhicheekpatra/Desktop/Technicolor_Projects/Dataset/Amazon_movies_Ratings_revenue_20_2011.txt"));
			BufferedReader br = new BufferedReader(new FileReader("dataset/Amazon/Amazon_movies_Ratings_revenue_20_2011.txt"));
			String line;
			while ((line = br.readLine()) != null) {
				Integer uid = Integer.parseInt(line.substring(0, line.indexOf("\t")));
				if(!userList.contains(uid)) {
					userList.add(uid);
					wholeProfiles.put(uid, new ScoreCount<Integer>());
					userProfiles.put(uid, new ScoreCount<Integer>());
					trainingProfiles.put(uid, new LinkedHashMap<Integer,Long>());
					receivedItemsList.put(uid, new ArrayList<Integer>());
					receivedItems.put(uid, new ArrayList<Integer>());
					ratings.put(uid, new ArrayList<Double>());

				}
				timelineUser.add(uid);
				line = line.substring(line.indexOf("\t")+1);
				Integer rid = Integer.parseInt(line.substring(0, line.indexOf("\t")));
				timelineItem.add(rid);
				if(!itemList.contains(rid)){
					ratingsItem.put(rid, new ArrayList<Double>());
					itemList.add(rid);
				}
				line = line.substring(line.indexOf("\t")+1);
				Double rating = Double.parseDouble(line.substring(0, line.indexOf("\t")));
				timelineRating.add(rating);
				ratings.get(uid).add(rating);
				ratingsItem.get(rid).add(rating);
				wholeProfiles.get(uid).addValue(rid,rating);

				line = line.substring(line.indexOf("\t")+1);
				Double price = Double.parseDouble(line.substring(0, line.indexOf("\t")));

				line = line.substring(line.indexOf("\t")+1);
				long time = Long.parseLong(line);
				timelineTime.add(time);

			}
			br.close();


		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void initTimeLineCiao() {
		try {
			//BufferedReader br = new BufferedReader(new FileReader("/Users/rhicheekpatra/Desktop/u.data"));
			BufferedReader br = new BufferedReader(new FileReader("dataset/ciao/ciao_20.txt"));
			String line;
			while ((line = br.readLine()) != null) {
				Integer uid = Integer.parseInt(line.substring(0, line.indexOf("\t")));
				if(!userList.contains(uid)) {
					userList.add(uid);
					wholeProfiles.put(uid, new ScoreCount<Integer>());
					userProfiles.put(uid, new ScoreCount<Integer>());
					trainingProfiles.put(uid, new LinkedHashMap<Integer,Long>());
					receivedItemsList.put(uid, new ArrayList<Integer>());
					receivedItems.put(uid, new ArrayList<Integer>());
					ratings.put(uid, new ArrayList<Double>());
				}
				timelineUser.add(uid);
				line = line.substring(line.indexOf("\t")+1);
				Integer rid = Integer.parseInt(line.substring(0, line.indexOf("\t")));
				timelineItem.add(rid);
				if(!itemList.contains(rid)){
					ratingsItem.put(rid, new ArrayList<Double>());
					itemList.add(rid);
				}

				line = line.substring(line.indexOf("\t")+1);
				Double rating = Double.parseDouble(line.substring(0, line.indexOf("\t")));
				timelineRating.add(rating);
				ratings.get(uid).add(rating);
				ratingsItem.get(rid).add(rating);
				wholeProfiles.get(uid).addValue(rid,rating);

				line = line.substring(line.indexOf("\t")+1);
				long time = Long.parseLong(line);
				timelineTime.add(time);

			}
			br.close();
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	private static void initTimeLine() {
		try {
			//BufferedReader br = new BufferedReader(new FileReader("/Users/rhicheekpatra/Desktop/u.data"));
			BufferedReader br = new BufferedReader(new FileReader("dataset/ml-100k/u.data"));
			String line;
			while ((line = br.readLine()) != null) {
				Integer uid = Integer.parseInt(line.substring(0, line.indexOf("\t")));
				if(!userList.contains(uid)) {
					userList.add(uid);
					wholeProfiles.put(uid, new ScoreCount<Integer>());
					userProfiles.put(uid, new ScoreCount<Integer>());
					trainingProfiles.put(uid, new LinkedHashMap<Integer,Long>());
					receivedItemsList.put(uid, new ArrayList<Integer>());
					receivedItems.put(uid, new ArrayList<Integer>());
					ratings.put(uid, new ArrayList<Double>());
				}
				timelineUser.add(uid);
				line = line.substring(line.indexOf("\t")+1);
				Integer rid = Integer.parseInt(line.substring(0, line.indexOf("\t")));
				timelineItem.add(rid);
				if(!itemList.contains(rid)){
					ratingsItem.put(rid, new ArrayList<Double>());
					itemList.add(rid);
				}

				line = line.substring(line.indexOf("\t")+1);
				Double rating = Double.parseDouble(line.substring(0, line.indexOf("\t")));
				timelineRating.add(rating);
				ratings.get(uid).add(rating);
				ratingsItem.get(rid).add(rating);
				wholeProfiles.get(uid).addValue(rid,rating);

				line = line.substring(line.indexOf("\t")+1);
				long time = Long.parseLong(line);
				timelineTime.add(time);

			}
			br.close();
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
