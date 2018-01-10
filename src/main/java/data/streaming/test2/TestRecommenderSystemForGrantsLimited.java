package data.streaming.test2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;
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
 * Extract a rating information SAMPLE for grants and generate a recommendation. 
 * The majority of grants have an unique direct possible keyword in a isolate cluster.
 * For the future, its necessary extract information about this keyword (US code)
 * from another place.
 * For test the recommender system generator, this process get a sample for each grant and
 * verify if the recommendations are consistent with the rest of data in the database.
 * 
 * It seems to works well, but grants more different with less association seems to have 
 * a little number of samples to recommend properly.  
 */

public class TestRecommenderSystemForGrantsLimited {
	
	private static final int MAX_RATINGS_WITH_VALUE = 3;
	
	private static final int RATINGS_WITH_1_VALUE = 7;
	
	private static final String PATH = "./out/grantRatingsData.csv";
	
	private static BufferedWriter buff;
	
	public static void main(String... args) throws IOException, RecommenderBuildException {
		
		MongoClientURI uri = new MongoClientURI("mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");
		
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());
		
		
		MongoCollection<org.bson.Document> grantsCollection = db.getCollection("grants");
		MongoCollection<org.bson.Document> grantsRatingsCollection = db.getCollection("grantRatings");
		
		// Extract all info in grants 
		List<Document> grants = (List<Document>) grantsCollection.find().into(new ArrayList<Document>());
		
		File file = new File(PATH);
		
		if(file.exists())
			file.delete();
		
		if (!file.exists() && !file.createNewFile())
			throw new IllegalArgumentException();
		
		buff = new BufferedWriter(new FileWriter(file));
		
		//Set<KeywordDTO> set = new HashSet<>();
		int printCount = 0;
		
		for(Document grant : grants) {
			
			// Search maximum MAX_RATINGS_WITH_VALUE ratings, RATINGS_WITH_1_VALUE with rating == 1
			String idGrant = grant.getString("idGrant");
			if(idGrant == null)
				continue;
			
			System.out.println("Creamos query");
			// Find rating for grantX with value > 1
			BasicDBObject orQuery = new BasicDBObject();
			List<BasicDBObject> orQuerySegments = new ArrayList<>();
			orQuerySegments.add(new BasicDBObject("idGrantA", idGrant));
			orQuerySegments.add(new BasicDBObject("idGrantB", idGrant));
			orQuery.put("$or", orQuerySegments);
			
			List<Document> grantRatingsDocuments = (List<Document>) grantsRatingsCollection.find(orQuery).into(new ArrayList<Document>());
			
			List<Document> result = new ArrayList<>();
			Integer grantRatingSize = grantRatingsDocuments.size();
			System.out.println("Found " + grantRatingSize + " results for grant: " + idGrant);
			
			if(grantRatingSize > MAX_RATINGS_WITH_VALUE) {
				if(grantRatingSize > 200) {
					List<Integer> randomIndex = new ArrayList<>();
					
					while(randomIndex.size() <= MAX_RATINGS_WITH_VALUE) {
						// Get a random sample 
						Integer random = ThreadLocalRandom.current().nextInt(0, grantRatingSize);
						if(!randomIndex.contains(random)) {
							randomIndex.add(random);
							result.add(grantRatingsDocuments.get(random));
						}
					}
				}else {
					Collections.shuffle(grantRatingsDocuments);
					result.addAll(grantRatingsDocuments.subList(0, MAX_RATINGS_WITH_VALUE));
				}
			}else {
				result.addAll(grantRatingsDocuments);
			}
			
			
			// Add samples with rating = 1 for negative feedback;
			// To gain space in the database, this negative rating isn't stored, so
			// it is necessary calculate this values again. It chooses a sample randomly
			
			Set<Document> grantRatingsDocuments1Value = new HashSet<>();
			while(grantRatingsDocuments1Value.size() <= RATINGS_WITH_1_VALUE) {
				Integer random = ThreadLocalRandom.current().nextInt(0, grants.size());
				
				Document grantSelectedRandomly = grants.get(random);
				
				List<String> keywords1 = clearKeywordsWithMassiveResults((List<String>) grant.get("keywords"));
				
				List<String> keywords2 = clearKeywordsWithMassiveResults((List<String>) grantSelectedRandomly.get("keywords"));
				
				
				Double rating = 0.0;
				if(keywords1.size() > 0 && keywords2.size() >0) {
					for(String k : keywords1) {
						if(keywords2.contains(k))
							rating += 2.0;
					}
					
					if(rating == 0.0) {
						Document ratingDocument = new Document();
						ratingDocument.put("idGrantA", grant.get("idGrant"));
						ratingDocument.put("idGrantB", grantSelectedRandomly.get("idGrant"));
						ratingDocument.put("rating", 1.0);
						
						grantRatingsDocuments1Value.add(ratingDocument);
					}
				
				}
			}
			
			System.out.println("Begin to write results");
			
			
			for(Document rating : result) {
				String grantA = rating.getString("idGrantA");
				String grantB = rating.getString("idGrantB");
				Double ratingD = rating.getDouble("rating");
				
				buff.write(grantA + "," + grantB + "," + ratingD.toString());
				buff.newLine();
				buff.write(grantB + "," + grantA + "," + ratingD.toString());
				buff.newLine();
				buff.flush();
	
			}
			for(Document rating : grantRatingsDocuments1Value) {
				String grantA = rating.getString("idGrantA");
				String grantB = rating.getString("idGrantB");
				Double ratingD = rating.getDouble("rating");
				
				buff.write(grantA + "," + grantB + "," + ratingD.toString());
				buff.newLine();
				buff.flush();

			}
			
			System.out.println("Results write completelly");
			System.out.println("-------------------------------");
		}
		buff.close();
		
		
		System.out.println("Step 1: Extraction of MLab data, done");
		System.out.println("----------------------------");
		Set<KeywordDTO> set = Utils.getKeywords();
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
	
	// 
	private static List<String> clearKeywordsWithMassiveResults(List<String> keywords){
		List<String> clearKeywords = new ArrayList<>();
		for(String keyword : keywords) {
			if(!keyword.contains(" ") && !keyword.equals("Incentivo")) {
				
				clearKeywords.add(keyword);
			}
		}
		return clearKeywords;
	}
	
}
