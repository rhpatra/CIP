import java.io.*;
import java.util.*;
import java.lang.*;
import java.text.DecimalFormat;


public class prepareInputD2V_sessions {

    public static LinkedHashMap<String,LinkedHashMap<String,Long>> userProfiles= new LinkedHashMap<String,LinkedHashMap<String,Long>>();
    

    public static void main (String[] args) throws Exception {
	prepareFile();
    }

    public static void prepareFile() throws Exception{
	//ml-1m
	//File file_ratings=new File("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-1m/ratings_tsv_sorted.dat");

	//ciao
	File file_ratings=new File("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ciao/ciao_20_sorted.txt");

	
	//ml-100k
	//File file_ratings=new File("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-100k/u_sorted.data");
	BufferedReader br = new BufferedReader(new FileReader(file_ratings));


	int count=0;
        String input;
        String[] parts;
        String rid,uid;
   	long prev_time=0,new_time=0;

        while((input = br.readLine()) != null){
		if(count%1000000==0) System.out.println(count+" events processed!");
                parts=input.split("\t");

                rid=parts[1];
                uid=parts[0];

                if(userProfiles.get(uid)==null) userProfiles.put(uid,new LinkedHashMap<String,Long>());

                userProfiles.get(uid).put(rid, Long.parseLong(parts[3]));

                count++;
	}

	//ml-1m
	//File file_categories=new File("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-1m/user_contents_sessions_1hr_ml1m.txt");

	//ciao
	File file_categories=new File("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ciao/user_contents_sessions_1hr_ciao.txt");

	//ml-100k
   	//File file_categories=new File("/Users/rhicheekpatra/Desktop/Gensim_Experiments/datasets/ml-100k/user_contents_sessions_1hr_ml100k.txt");
        FileWriter fw = new FileWriter(file_categories);
        BufferedWriter bw = new BufferedWriter(fw);

   	for(String userid: userProfiles.keySet()){
		prev_time=0;
   		if(userProfiles.get(userid).keySet().size()>3){
   			for(String item: userProfiles.get(userid).keySet()){
   				new_time=userProfiles.get(userid).get(item);
   				if(new_time-prev_time>86400){
   					bw.write("\n"+userid+"\t");
   				}
   				bw.write(item+",");
   				 
   				prev_time=new_time;
   			 }
   	 
   			 //bw.write("\n");
   			 //bw.write(userid+"\t"+userProfiles.get(userid).toString().replace("[", "").replace("]", "")+"\n");
   		 
   		 }
   	 }

   	 bw.close();
   	 

    }

}

