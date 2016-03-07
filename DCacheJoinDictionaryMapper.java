package DCacheJoin;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class DCacheJoinDictionaryMapper  extends Mapper<Text, Text, Text, Text> {
 	String fileName=null, language=null;
	   public Map<String, String> translations = new HashMap<String, String>();
	  private BufferedReader brReader;
	   @SuppressWarnings("deprecation")   
	   public void setup(Context context) throws IOException, InterruptedException{
		// TODO: determine the name of the additional language based on the file name  
		try{   
		   Path[] dataFile = new Path[0];
		   dataFile = context.getLocalCacheFiles();
		   for (Path eachPath : dataFile) {
			   fileName = eachPath.getName().toString().trim();
			   System.out.println("fileName : "+fileName);
			   language = fileName.substring(0,fileName.length()-4);
			   System.out.println("language: "+ language);
			   createLanguageFileHashMap(eachPath);
		   }
		} catch(IOException e){
			System.err.println("Exception reading cache file:" +e);
		}
		   
		   
           
	    // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1
           
	   }
	   private void createLanguageFileHashMap(Path filePath) throws IOException {
		  try{
		   String strLineRead = null;
		   brReader = new BufferedReader(new FileReader(filePath.toString()));
		   while ((strLineRead = brReader.readLine()) != null) {
			   if(!(strLineRead.toString().charAt(0)=='#') && (strLineRead.toString().indexOf('['))>0){
				   String[] lineSplit = strLineRead.split("\t");
				   if(lineSplit.length!=0 && lineSplit.length!=1 ){
					System.out.println("length:"+lineSplit.length);   
					String englishWord = lineSplit[0];
				        String[] splitValue=lineSplit[1].toString().split("\\[");
	                   	        if(splitValue.length!=0 && splitValue.length!=1 ){
	                    			String partsOfSpeech = splitValue[1].substring(0,splitValue[1].length()-1);
	                    			if(valid(partsOfSpeech)){
	                    				String key= englishWord + " : ["+partsOfSpeech+"]";
	                           			 String value = language+ ":"+splitValue[0];
	                           			System.out.println("key: "+key);
							System.out.println("value: "+value);
							 translations.put(key, value);
	                    			}
	                   		 }
		 		 }
			   }
		   }
		} catch (IOException ioe){
			System.err.println("Exception while reading cache file!"+ ioe.toString());
		}
		  
		 brReader.close();
	}
			
   private boolean valid(String partsOfSpeech) {
		String[] words = {"Noun", "Pronoun", "Verb", "Adverb", "Adjective", "Preposition", "Article", "Conjunction"};  
	    return (Arrays.asList(words).contains(partsOfSpeech));
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file
		String appendValue=null;
		System.out.println("Searching for key: "+key);
		System.out.println("And value: "+value);	
	       	if (translations.containsKey(key.toString())){
			System.out.println("match");
			appendValue= value.toString()+" | "+translations.get(key.toString());
			System.out.println("append Value"+appendValue);

		}
		else{
			appendValue=value.toString()+" | "+language+":N/A";
		}	
		context.write(new Text(key), new Text(appendValue));
			
	      }

   }
