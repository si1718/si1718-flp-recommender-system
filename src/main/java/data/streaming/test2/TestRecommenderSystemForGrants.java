package data.streaming.test2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.KeywordDTO;
import data.streaming.utils.Utils;

/**
 * Extract all rating information for grants and generate a recommendation. 
 * This process need a lot of memory and returns recommendations useless
 * because the majority of grants have a unique direct possible keyword.
 * For the future, its necessary extract iformaton about this keyword (US code)
 * from another place.
 */

public class TestRecommenderSystemForGrants {
	
	private static final String PATH = "./out/grantRatingsData.csv";
	
	private static BufferedWriter buff;
	
	public static void main(String... args) throws IOException, RecommenderBuildException {
		
		MongoClientURI uri = new MongoClientURI("mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");
		
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());
		
		
		// Extract info in grantRatings
		MongoCollection<org.bson.Document> grantsRatingsCollection = db.getCollection("grantRatings");
		
		List<org.bson.Document> grantRatingsDocuments = (List<org.bson.Document>) grantsRatingsCollection.find().into(new ArrayList<org.bson.Document>());
		System.out.println("Step 1: Extraction of MLab data, done");
		System.out.println("----------------------------");
		
		File file = new File(PATH);
		
		if(file.exists())
			file.delete();
		
		if (!file.exists() && !file.createNewFile())
			throw new IllegalArgumentException();
		
		buff = new BufferedWriter(new FileWriter(file));
		
		Set<KeywordDTO> set = new HashSet<>();
		
		int printCount = 0;
		for(org.bson.Document rating : grantRatingsDocuments) {
			String grantA = rating.getString("idGrantA");
			String grantB = rating.getString("idGrantB");
			Double ratingD = rating.getDouble("rating");
			
			// write in a csv file for security
			buff.write(grantA + "," + grantB + "," + ratingD.toString());
			buff.newLine();
			buff.flush();
			
			set.add(new KeywordDTO(grantA, grantB, ratingD));
			
			if(printCount <= set.size()) {
				System.out.println("Processed " + set.size() + " ratings");
				System.out.println("-----------------------");
				printCount += 500;
			}
				
		}
		buff.close();
		
		
		// Set<KeywordDTO> set = Utils.getKeywords();
		System.out.println("Step 2: Extraction of the 'ratings of grants' objects from the data, done");
		System.out.println("----------------------------");
		ItemRecommender irec = Utils.getRecommender(set);
		System.out.println("Step 3: Generation of the recommendation system based on the rating, done");
		System.out.println("----------------------------");
		Utils.saveModelInMLab(irec, set);
		System.out.println("Step 4: Dump of the results of the recommendation system in MLab, done");
		System.out.println("----------------------------");
		System.out.println("Finish!!");
	}
	
}
