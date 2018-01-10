package data.streaming.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.bson.Document;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.Recommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventCollectionDAO;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;
import org.grouplens.lenskit.scored.ScoredId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.KeywordDTO;

public class Utils {

	public static final String PROPERTIES_FILE = "resources/data.properties";
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final int MAX_RECOMMENDATIONS = 3;

	public static ItemRecommender getRecommender(Set<KeywordDTO> dtos) throws RecommenderBuildException {
		LenskitConfiguration config = new LenskitConfiguration();
		EventDAO myDAO = EventCollectionDAO.create(createEventCollection(dtos));

		config.bind(EventDAO.class).to(myDAO);
		config.bind(ItemScorer.class).to(UserUserItemScorer.class);
		// config.bind(BaselineScorer.class,
		// ItemScorer.class).to(UserMeanItemScorer.class);
		// config.bind(UserMeanBaseline.class,
		// ItemScorer.class).to(ItemMeanRatingItemScorer.class);

		Recommender rec = LenskitRecommender.build(config);
		return rec.getItemRecommender();
	}

	private static Collection<? extends Event> createEventCollection(Set<KeywordDTO> ratings) {
		List<Event> result = new LinkedList<>();

		for (KeywordDTO dto : ratings) {
			MutableRating r = new MutableRating();
			r.setItemId(dto.getKey1().hashCode());
			r.setUserId(dto.getKey2().hashCode());
			r.setRating(dto.getStatistic());
			result.add(r);
		}
		return result;
	}

	public static Set<KeywordDTO> getKeywords() throws IOException {
		Set<KeywordDTO> result = new HashSet<>();
		BufferedReader reader = new BufferedReader(new FileReader("out/grantRatingsData.csv"));
		String line = reader.readLine();
		//int count = 0;
		while (line != null) {
			String[] splits = line.split(",");
			if (splits.length == 3) {
				result.add(new KeywordDTO(splits[0], splits[1], Double.valueOf(splits[2])));
			}
			line = reader.readLine();
			/*count++;
			System.out.println(count);*/
		}

		reader.close();
		return result;
	}

	public static void saveModel(ItemRecommender irec, Set<KeywordDTO> set) throws IOException {
		Map<String, Long> keys = Maps.asMap(set.stream().map((KeywordDTO x) -> x.getKey1()).collect(Collectors.toSet()),
				(String y) -> new Long(y.hashCode()));
		Map<Long, List<String>> reverse = set.stream().map((KeywordDTO x) -> x.getKey1())
				.collect(Collectors.groupingBy((String x) -> new Long(x.hashCode())));

		BufferedWriter writer = new BufferedWriter(new FileWriter("out/grantRatingsModel.csv"));

		for (String key : keys.keySet()) {
			List<ScoredId> recommendations = irec.recommend(keys.get(key), MAX_RECOMMENDATIONS);
			if (recommendations.size() > 0) {
				writer.append(key + "->" + recommendations.stream().map(x -> reverse.get(x.getId()).get(0))
						.collect(Collectors.toList()));
				writer.newLine();
			}
			writer.flush();
		}

		writer.close();

	}
	
	public static void saveModelInMLab(ItemRecommender irec, Set<KeywordDTO> set) throws IOException {
		
		MongoClientURI uri = new MongoClientURI("mongodb://admin:passwordCurro@ds135917.mlab.com:35917/grant-recommendations");
		
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());
		
		
		Map<String, Long> keys = Maps.asMap(set.stream().map((KeywordDTO x) -> x.getKey1()).collect(Collectors.toSet()),
				(String y) -> new Long(y.hashCode()));
		Map<Long, List<String>> reverse = set.stream().map((KeywordDTO x) -> x.getKey1())
				.collect(Collectors.groupingBy((String x) -> new Long(x.hashCode())));
		
		
		MongoCollection<org.bson.Document> grantRecommendationsCollection = db.getCollection("recommendations");
		
		grantRecommendationsCollection.deleteMany(new Document());
		
		List<Document> grantRecommendationsDocuments = new ArrayList<>();
		for (String key : keys.keySet()) {
			List<ScoredId> recommendations = irec.recommend(keys.get(key), MAX_RECOMMENDATIONS+1);
			if (recommendations.size() > 0) {
				
				List<String> recommendationStringList = new ArrayList<String>();
				
				for(ScoredId rec : recommendations) {
					String recStr = reverse.get(rec.getId()).get(0);
					if(!recStr.equals(key) && recommendationStringList.size() != MAX_RECOMMENDATIONS)
						recommendationStringList.add(recStr);
				}
				
				Document recommendationDocument = new Document();
				recommendationDocument.append("idGrant", key);
				recommendationDocument.append("recommendations", recommendationStringList);
				
				grantRecommendationsDocuments.add(recommendationDocument);
			}
			
		}
		
		grantRecommendationsCollection.insertMany(grantRecommendationsDocuments);
		client.close();

	}

}
