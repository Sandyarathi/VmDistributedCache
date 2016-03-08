package DCacheJoin;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
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
		try{   
		   Path[] dataFile = new Path[0];
		   dataFile = context.getLocalCacheFiles();
		   for (Path eachPath : dataFile) {
			   fileName = eachPath.getName().toString().trim();
			   language = fileName.substring(0,fileName.length()-4);
			   createLanguageFileHashMap(eachPath);
		   }
		} catch(IOException e){
			System.err.println("Exception reading cache file:" +e);
		}
		   
	}
	private void createLanguageFileHashMap(Path filePath) throws IOException {
		try{
		   String strLineRead = null;
		   brReader = new BufferedReader(new FileReader(filePath.toString()));
		   while ((strLineRead = brReader.readLine()) != null) {
			   if(!(strLineRead.toString().charAt(0)=='#') && (strLineRead.toString().indexOf('['))>0){
				   String[] lineSplit = strLineRead.split("\t");
				   if(lineSplit.length!=0 && lineSplit.length!=1 ){
						String englishWord = lineSplit[0];
						if(lineSplit[1].indexOf('[')>0){
							String partsOfSpeech = lineSplit[1].substring(lineSplit[1].lastIndexOf('[')+1,lineSplit[1].length()-1);
			      	 		String translationValues= lineSplit[1].substring(0,lineSplit[1].lastIndexOf('[') );
				        	if(valid(partsOfSpeech)){
	           					String key= englishWord + " : ["+partsOfSpeech+"]";
	           					construct(key, translationValues);
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
	
	private void construct(String key, String value){
		if(translations.containsKey(key)){
			String currentValue = translations.get(key);
			currentValue+=", ";
			currentValue+=value;
			translations.put(key, currentValue);
		}
		else
			translations.put(key, language+":"+value);
	}
			
   private boolean valid(String partsOfSpeech) {
		String[] words = {"Noun", "Pronoun", "Verb", "Adverb", "Adjective", "Preposition", "Article", "Conjunction", "interjection"};  
	    return (Arrays.asList(words).contains(partsOfSpeech));
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String appendValue=null;
       	if (translations.containsKey(key.toString())){
			appendValue= value.toString()+" | "+translations.get(key.toString());

		}
		else{
			appendValue=value.toString()+" | "+language+":N/A";
		}	
		context.write(new Text(key), new Text(appendValue));
			
	}

}
