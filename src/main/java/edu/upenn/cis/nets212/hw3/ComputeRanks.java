package edu.upenn.cis.nets212.hw3;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVParser;
import edu.upenn.cis.nets212.config.Config;
import edu.upenn.cis.nets212.storage.DynamoConnector;
import edu.upenn.cis.nets212.storage.SparkConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.config.Log4j1ConfigurationParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import edu.upenn.cis.nets212.config.Config;
import edu.upenn.cis.nets212.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class ComputeRanks {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ComputeRanks.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	DynamoDB db;
	Table table;
	
	static JavaSparkContext context;
	
	public ComputeRanks() {
		System.setProperty("file.encoding", "UTF-8");
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");
		db = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		
		
		/*DynamoDBMapperConfig mapperConfig = new DynamoDBMapperConfig.Builder().withTableNameOverride(DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement("Table_Name")).build();

		DynamoDBMapper mapper = new DynamoDBMapper(client, mapperConfig);

		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();

		// Change to your model class   
		List < ParticipantReport > scanResult = mapper.scan(ParticipantReport.class, scanExpression);*/

		
    //  Load the file filePath into an RDD (take care to handle both spaces and tab characters as separators)
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split("[ \\t]"));
			
		JavaPairRDD<Integer, Integer> pairsOut = file.mapToPair(line -> new Tuple2<Integer, Integer>(Integer.parseInt(line[0]), Integer.parseInt(line[1])));
		
		 JavaPairRDD<Integer, Integer> pairsIn = pairsOut.mapToPair(line -> new Tuple2<Integer, Integer>(line._2, line._1));
		// maps nodeId to indegree
		 System.out.println("size pairsIn: " + pairsIn.count());
		 System.out.println("size pairsOut: " + pairsOut.count());
		JavaPairRDD<Integer, Integer> pairOutCounts = pairsOut.aggregateByKey(0,
				(val,row) -> val + 1, (val,val2) -> val+val2);
		JavaPairRDD<Integer, Integer> pairInCounts = pairsIn.aggregateByKey(0,
				(val,row) -> val + 1, (val,val2) -> val+val2);
		long numV = pairOutCounts.cogroup(pairInCounts).count();
			
		logger.info("vertices: " + numV);
		return pairsOut;
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {

		JavaPairRDD<Integer, Integer> pairsIn = network.mapToPair(line -> new Tuple2<Integer, Integer>(line._2, line._1));
		JavaPairRDD<Integer, Integer> pairOutCounts = network.aggregateByKey(0,
				(val,row) -> val + 1, (val,val2) -> val+val2);
		JavaPairRDD<Integer, Integer> pairInCounts = pairsIn.aggregateByKey(0,
				(val,row) -> val + 1, (val,val2) -> val+val2);
		JavaPairRDD<Integer, Integer> sinkPairs = pairInCounts.subtractByKey(pairOutCounts);
		JavaRDD<Integer> sinks = sinkPairs.map(tuple -> tuple._1);
		
		return sinks;
	}
	
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			
			table = db.createTable("adsorption", Arrays.asList(new KeySchemaElement("url", KeyType.HASH), // Partition key
							new KeySchemaElement("username", KeyType.RANGE)), Arrays.asList(new AttributeDefinition("url", ScalarAttributeType.S),
									new AttributeDefinition("username", ScalarAttributeType.S)),
							new ProvisionedThroughput(25L, 25L));
			table.waitForActive();
		} catch (final ResourceInUseException exists) {
			table = db.getTable("adsorption");
		}

	}
	
	void writeToTable(JavaPairRDD<String, Tuple2<String, Double>> data) {
		data.foreachPartition((partition) -> {
			DynamoDB docClient = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
			TableWriteItems tbItems = new TableWriteItems(table.getTableName());
			Set<String> used = new HashSet<>();
			short cnt = 0;
			while (partition.hasNext()) {
				Tuple2<String, Tuple2<String, Double>> x = partition.next();
				String url = x._1;
				String username = x._2._1;
				if (used.add(username + "$" + url)) {
					Double weight = x._2._2;
					Item item = new Item().withPrimaryKey("url", url)
							.withString("username", username)
							.withNumber("weight", weight);
					tbItems.addItemToPut(item);
					++cnt;
					if (cnt == 25) {
						BatchWriteItemOutcome outcome = docClient.batchWriteItem(tbItems);
						while (outcome.getUnprocessedItems().size() > 0) {
							outcome = docClient.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
						}
						cnt = 0;
						tbItems = new TableWriteItems(table.getTableName());
					}
				}
			}
			if (cnt > 0) {
				BatchWriteItemOutcome outcome = docClient.batchWriteItem(tbItems);
				while (outcome.getUnprocessedItems().size() > 0) {
					outcome = docClient.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
				}
			}
		});
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run() throws IOException, InterruptedException {
		
		//Scan table for friends: gets user nodes and (u, u) edges
		String friendsTableName = "friends";
	    List<Tuple2<String, String>> friendEdgeList = new ArrayList<>();
	    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();
	    ScanRequest scanRequest = new ScanRequest()
	        .withTableName(friendsTableName);
	    ScanResult result = dynamoDBClient.scan(scanRequest);
	    for (Map<String, AttributeValue> item : result.getItems()) {
	        for (String s: item.get("friends").getSS()) {
	        	if (!s.equals(item.get("username").getS())) {
	        		friendEdgeList.add(new Tuple2<String, String>(item.get("username").getS(), s));
	        	}
	        }
	    }
	    JavaRDD<Tuple2<String, String>> friendEdges = context.parallelize(friendEdgeList);
	    JavaPairRDD<String, String> friendEdgesRDD = friendEdges.mapToPair(
	    		x -> new Tuple2<String, String>(x._1, x._2));
	    JavaPairRDD<String, Integer> friendCountRDD = friendEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> friendTransferRDD = friendCountRDD.mapValues(i -> (double)0.3/i);
	    
	    //edges between (u1, u2) = (u1, (u2, scale)) and (u2, u1)
	    JavaPairRDD<String, Tuple2<String, Double>> friendEdgeWeightRDD = friendEdgesRDD.join(friendTransferRDD);
	    
	    
	    
	    
	    
	    
	    //Scan table for newslikes: add edges b/w users, urls
	    String likesTableName = "newslikes";
	    List<Tuple2<String, String>> likesList = new ArrayList<>();
	    //List<Tuple2<String, String>> articleCategoryList = new ArrayList<>();
	    ScanRequest likesScanRequest = new ScanRequest()
	        .withTableName(likesTableName);
	    ScanResult likesResult = dynamoDBClient.scan(likesScanRequest);
	    //scan table for (username, liked article)
	    for (Map<String, AttributeValue> item : likesResult.getItems()) {
	    	likesList.add(new Tuple2<String, String>(item.get("userid").getS(), item.get("url").getS()));
//	    	articleCategoryList.add(new Tuple2<String, String>(item.get("category").getS(),
//	    			item.get("url").getS()));
	    }
	    JavaRDD<Tuple2<String, String>> likeEdges = context.parallelize(likesList);
	    JavaPairRDD<String, String> likeEdgesRDD = likeEdges.mapToPair(x -> 
	    new Tuple2<String, String>(x._1, x._2));
	    JavaPairRDD<String, Integer> numLikesRDD = likeEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> likesPropRDD = numLikesRDD.mapValues(i -> (double)(0.4)/i);
	    //edges between (u, a) = (u, (a, scale factor))
	    JavaPairRDD<String, Tuple2<String, Double>> uaEdgeWeightRDD = likeEdgesRDD.join(likesPropRDD);
//	    for (Tuple2<String, Tuple2<String, Double>> t: uaEdgeWeightRDD.collect()) {
//	    	System.out.println(t._1 + "->" + t._2._1 + "\t" + t._2._2);
//	    }
	    
	    JavaPairRDD<String, String> likedEdgesRDD= likeEdges.mapToPair(x -> 
	    	new Tuple2<String, String>(x._2, x._1));
	    JavaPairRDD<String, Integer> numLikedRDD = likedEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> likedPropRDD = numLikedRDD.mapValues(i -> (double)(0.4)/i);
	    //edges between (a, u) = (a, (u, scale factor))
	    JavaPairRDD<String, Tuple2<String, Double>> auEdgeWeightRDD = likedEdgesRDD.join(likedPropRDD);
	    
	    
//	    JavaRDD<Tuple2<String, String>> articleCategoryEdges = context.parallelize(articleCategoryList);
//	    //for edge a.1 -> a.2 or user -> url, make (url, user) or (a.2, a.1)
//	    JavaPairRDD<String, String> acEdgesRDD = articleCategoryEdges.mapToPair(
//	    		x -> new Tuple2<String, String>(x._2, x._1));
//	    JavaPairRDD<String, Integer> numACEdges = acEdgesRDD.mapValues(x -> 1).reduceByKey((x, y) -> (x+y));
	    
	    
	    
	    //Scan table for users: add edges b/w users and interests
	    String usersTableName = "users";
	    List<Tuple2<String, String>> userInterestList = new ArrayList<>();
	    ScanRequest userScanRequest = new ScanRequest()
	        .withTableName(usersTableName);
	    ScanResult usersResult = dynamoDBClient.scan(userScanRequest);
	    //scan table for (username, interests)
	    for (Map<String, AttributeValue> item : usersResult.getItems()) {
	    	for (String s: item.get("interests").getSS()) {
	    		userInterestList.add(new Tuple2<String, String>(item.get("username").getS(), s));
	        }
	    }
	    JavaPairRDD<String, String> userInterestsRDD = context.parallelizePairs(userInterestList);
	    JavaPairRDD<String, Integer> numUserInterestsRDD = userInterestsRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
	    JavaPairRDD<String, Double> uiPropRDD = numUserInterestsRDD.mapValues(i -> (double)(0.3)/i);
	    //edges between (u, (c, scale))
	    JavaPairRDD<String, Tuple2<String, Double>> ucEdgeWeightRDD = userInterestsRDD.join(uiPropRDD);
	    
	    JavaPairRDD<String, String> categoryUserRDD = userInterestsRDD.mapToPair(x ->
	    	new Tuple2<String, String>(x._2, x._1));
	    JavaPairRDD<String, Integer> numCategoryUsersRDD = categoryUserRDD.mapValues(x->1).reduceByKey((x,y) -> (x+y));
	    JavaPairRDD<String, Double> cuPropRDD = numCategoryUsersRDD.mapValues(i -> (double)(0.3)/i);
	    //edges between (c, (u, scale))
	    JavaPairRDD<String, Tuple2<String, Double>> cuEdgeWeightRDD = categoryUserRDD.join(cuPropRDD);
	    
	    
	    
	    
	    //Scan table for news: (article, category)
	    String newsTableName = "fakenews";
	    List<Tuple2<String, String>> articleCategoryList = new ArrayList<>();
	    ScanRequest newsScanRequest = new ScanRequest().withTableName(newsTableName);
	    ScanResult newsResult = dynamoDBClient.scan(newsScanRequest);
	    for (Map<String, AttributeValue> item: newsResult.getItems()) {
	    	articleCategoryList.add(new Tuple2<>(item.get("url").getS(), item.get("category").getS()));
	    }
	    JavaPairRDD<String, String> articleCategoryRDD = context.parallelizePairs(articleCategoryList);
	    //edges between (a, (c, scale))
	    JavaPairRDD<String, Tuple2<String, Double>> acEdgeWeightRDD = articleCategoryRDD.mapToPair(x ->
	    		new Tuple2<>(x._1, new Tuple2<>(x._2, (double)(1.0))));
	    
	    JavaPairRDD<String, String> categoryArticleRDD = articleCategoryRDD.mapToPair(x -> new Tuple2<>(x._2, x._1));
	    JavaPairRDD<String, Integer> numCategoryArticleRDD = categoryArticleRDD.mapValues(x->1).reduceByKey((x,y)->(x+y));
	    JavaPairRDD<String, Double> caPropRDD = numCategoryArticleRDD.mapValues(i -> (double)(1.0)/i);
	    //edges betwee (c, (a, scale))
	    JavaPairRDD<String, Tuple2<String, Double>> caEdgeWeightRDD = categoryArticleRDD.join(caPropRDD);

	    JavaPairRDD<String, Tuple2<String, Double>> edges = friendEdgeWeightRDD.union(uaEdgeWeightRDD)
	    		.union(auEdgeWeightRDD).union(ucEdgeWeightRDD).union(cuEdgeWeightRDD).union(acEdgeWeightRDD).union(caEdgeWeightRDD);
	    
	    JavaPairRDD<String, Tuple2<String, Double>> userRDD = numUserInterestsRDD.mapToPair(
		x -> new Tuple2<String, Tuple2<String, Double>>(x._1, new Tuple2<String, Double>(x._1, 1.0)));
	    
	    JavaPairRDD<Tuple2<String, String>, Double> userHardCode = userRDD.mapToPair(x -> 
	    		new Tuple2<>(new Tuple2<>(x._1, x._1), (double)1.0));
	    
	    JavaPairRDD<String, Tuple2<String, Double>> nodeRDD = numUserInterestsRDD.mapToPair(
	    		x -> new Tuple2<String, Tuple2<String, Double>>(x._1, new Tuple2<String, Double>(x._1, 1.0)));
	    
	    JavaPairRDD<Tuple2<String, String>, Double> labeledNodes = nodeRDD.mapToPair(x ->
	    		new Tuple2<>(new Tuple2<>(x._1, x._2._1), x._2._2));
	    
	    for (int i = 0; i < 15; ++i) {
	    	System.out.println("Running iteration: " + i);
	    	//propogate along edge weights
	    	//send to node (b, (label, weight))
	    	JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Tuple2<String, Double>>> prop = nodeRDD.join(edges);
	    	JavaPairRDD<String, Tuple2<String, Double>> newNodes = prop.mapToPair(x -> new Tuple2<>
	    		(x._2._2._1, new Tuple2<>(x._2._1._1,x._2._1._2 * x._2._2._2)));
    	
	    	//exclude labels only only include node and weight
	    	JavaPairRDD<String, Double> extractDouble = newNodes.mapToPair(x -> new Tuple2<>(x._1, x._2._2));
	    	//sum up all weights for a particular node regardless of label
	    	JavaPairRDD<String, Double> sumOfNodes = extractDouble.reduceByKey((x, y) -> (x+y));
	    	//normalize nodes and labels by weight such that all nodes sum to 1
	    	JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Double>> normalize = newNodes.join(sumOfNodes);
	    	newNodes = normalize.mapToPair(x -> new Tuple2<>(x._1, new Tuple2<>(x._2._1._1, x._2._1._2/x._2._2)));
	    	//map to ((node, label), weight) and reduce by key to sum for same (node, label) pair/convert back
	    	JavaPairRDD<Tuple2<String, String>, Double> mergeSameLabel = newNodes.mapToPair(x ->
	    			new Tuple2<>(new Tuple2<>(x._1, x._2._1),( x._2._2)));
		    
	    	//merges labels with same value
	    	mergeSameLabel = mergeSameLabel.reduceByKey((x, y)->(x+y));
	    	//hard set node with own label to 1
	    	mergeSameLabel = mergeSameLabel.subtractByKey(userHardCode);
	    	mergeSameLabel = mergeSameLabel.union(userHardCode);
	    	
	    	//compute difference between labeled nodes in last round and current round
	    	JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Double>> pairDiff = labeledNodes.join(mergeSameLabel);
	    	JavaRDD<Double> diff = pairDiff.map(t -> Math.abs(t._2._1-t._2._2));
	    	
	    	//if every difference is below a certain threshold, stop
	    	if (i > 0) {
	    		boolean stop = diff.filter(x -> x > 0.15).count() == 0;
		    	if (stop) {
		    		break;
		    	}
	    	}
	    	
	    	//set current labeled nodes to previous
	    	labeledNodes = mergeSameLabel;
	    	
	    	
	    	//convert back to (node, (label, weight))
	    	newNodes = mergeSameLabel.mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2)));
	    	
	    	
	    	
	    	nodeRDD = newNodes;
		    	
	    }
	    logger.info("Initializing tables...");
	    initializeTables();
	    logger.info("Writing to database");
	    writeToTable(nodeRDD);
//	    
//	    for (Tuple2<String, Tuple2<String, Double>> t: edges.collect()) {
//	    	System.out.println(t._1 + "->" + t._2._1 + "\t" + t._2._2);
//	    }
	    
	    
	    
	    
	    
	    
//	    JavaPairRDD<String, Tuple2<String, Double>> userRDD = numUserInterestsRDD.mapToPair(
//	    		x -> new Tuple2<String, Tuple2<String, Double>>(x._1, new Tuple2<String, Double>(x._1, 1.0)));
//	    
//	    JavaPairRDD<String, Tuple2<Tuple2<String, Double>, String>> propRDD = userRDD.join(likeEdgesRDD);
	    
	    
	    
	    
	    
	    
	    
	        
	    /*String newsTableName = "news";

	    ArrayList<String> newsList = new ArrayList<String>();

	    ScanRequest newsScanRequest = new ScanRequest()
	        .withTableName(newsTableName);
	    ScanResult newsResult = dynamoDBClient.scan(newsScanRequest);
	    
	    List<String> newsUrls = new ArrayList<String>();

	    for (Map<String, AttributeValue> item : newsResult.getItems()) {
	    	newsList.add(item.toString());
	        System.out.println(item.toString());
	        newsUrls.add(item.get("url").getS());
	    }
	    
	    JavaRDD<String> urlsRDD = context.parallelize(newsUrls);*/
	    
	   /* String friendsTableName = "friends";

	    ArrayList<String> friendsList = new ArrayList<String>();

	    ScanRequest friendsScanRequest = new ScanRequest()
	        .withTableName(friendsTableName);
	    ScanResult friendsResult = dynamoDBClient.scan(friendsScanRequest);

	    List<String> users = new ArrayList<String>();
	    for (Map<String, AttributeValue> item : result.getItems()) {
	    	friendsList.add(item.toString());
	        // System.out.println(item.toString());
	        System.out.println(item.get("username").getS());
	        users.add(item.get("username").getS());
	    }
	    
	    JavaRDD<String> usersRDD = context.parallelize(users);
	    
	    String[] categoriesArray = {
	    		"POLITICS",
	        "WELLNESS",
	        "ENTERTAINMENT",
	        "TRAVEL",
	        "STYLE & BEAUTY",
	        "PARENTING",
	        "HEALTHY LIVING",
	        "QUEER VOICES",
	        "FOOD & DRINK",
	        "BUSINESS",
	        "COMEDY",
	        "SPORTS",
	        "BLACK VOICES",
	        "HOME & LIVING",
	        "PARENTS",
	        "THE WORLDPOST",
	        "WEDDINGS",
	        "WOMEN",
	        "IMPACT",
	        "DIVORCE",
	        "CRIME",
	        "MEDIA",
	        "WEIRD NEWS",
	        "GREEN",
	        "WORLDPOST",
	        "RELIGION",
	        "STYLE",
	        "SCIENCE",
	        "WORLD NEWS",
	        "TASTE",
	        "TECH",
	        "MONEY",
	        "ARTS",
	        "FIFTY",
	        "GOOD NEWS",
	        "ARTS & CULTURE",
	        "ENVIRONMENT",
	        "COLLEGE",
	        "LATINO VOICES",
	        "CULTURE & ARTS",
	        "EDUCATION"};
		
	    List<String> categories = Arrays.asList(categoriesArray);
	    
	    JavaRDD<String> categoriesRDD = context.parallelize(categories);*/
		/*logger.info("Running");
		
		// Load the social network
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(Config.SOCIAL_NET_PATH).distinct();
		
		logger.info("numEdges:" +  network.count());

		// add sinks
		JavaRDD<Integer> sinks = getSinks(network).distinct();

		// add back-edges
		network = addBacklinks(network, sinks);

		JavaPairRDD<Integer, Double> pageRank = computeSocialRanks(network);
		getTopTen(pageRank);

		logger.info("*** Finished social network ranking! ***");*/
	}
	public JavaPairRDD<Integer, Integer> addBacklinks(JavaPairRDD<Integer, Integer> network, JavaRDD<Integer> sinks) {

		JavaPairRDD<Integer, Integer> inverted = network.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._2, tuple._1));
		JavaPairRDD<Integer, Integer> sinkEdges = sinks.mapToPair(node -> new Tuple2<Integer, Integer>(node, node));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> backlinks = inverted.join(sinkEdges);
		logger.info("Number of backlinks added: " + backlinks.count());
		JavaPairRDD<Integer, Integer> backlinksTuple = backlinks.mapToPair(tuple -> new Tuple2<Integer, Integer>(tuple._1, tuple._2._1));
		return network.union(backlinksTuple);
	}
	public void getTopTen(JavaPairRDD<Integer, Double> pageRank) {
		JavaPairRDD<Double, Integer> reversed = pageRank.mapToPair(tuple -> new Tuple2<Double, Integer>(tuple._2, tuple._1));
		reversed = reversed.sortByKey(false);
		JavaPairRDD<Integer, Double> sorted = reversed.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._2, tuple._1));
		List<Tuple2<Integer, Double>> topTen = sorted.take(10);
		logger.info(topTen);
	}
	
	public JavaPairRDD<Integer, Double> computeSocialRanks(JavaPairRDD<Integer, Integer> network) {

		int count = 0;
		JavaPairRDD<Integer, Integer> outDegrees = network.aggregateByKey(0, 
				(val,row) -> val + 1, (val,val2) -> val+val2);
		JavaPairRDD<Integer, Double> nodeTransferRdd = outDegrees.mapToPair(tuple -> 
				new Tuple2<Integer, Double>(tuple._1, 1.0 / tuple._2));
		JavaPairRDD<Integer, Double> prevPageRank = 
				nodeTransferRdd.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._1, 1.0));
		if (imax == -1) {
			imax = 25;
		}
		if (dmax == -1) {
			dmax = 30;
		}
		while (count < imax) {
			JavaPairRDD<Integer, Double> currPageRank = computeSocialRank(network, prevPageRank);
			JavaPairRDD<Integer, Tuple2<Double, Double>> joinedPageRank = currPageRank.join(prevPageRank);
			JavaPairRDD<Integer, Double> differencePageRank = joinedPageRank.mapToPair(tuple -> 
					new Tuple2<Integer, Double>(tuple._1, Math.abs(tuple._2._1 - tuple._2._2)));
			JavaPairRDD<Integer, Double> smallDiffNodes = differencePageRank.filter(tuple -> tuple._2 >= dmax);
			if (debug) {
				logger.info(prevPageRank.collect());
			}
			prevPageRank = currPageRank;
			if (smallDiffNodes.isEmpty()) {
				break;
			}
			count++;
		}
		return prevPageRank;
	}
	
	public JavaPairRDD<Integer, Double> computeSocialRank(JavaPairRDD<Integer, Integer> network, JavaPairRDD<Integer, Double> pageRankRdd) {
		// mapping node to number of outdegrees
		JavaPairRDD<Integer, Integer> outDegrees = network.aggregateByKey(0, 
				(val,row) -> val + 1, (val,val2) -> val+val2);
		// getting pairrdds necessary for page rank calculations
		JavaPairRDD<Integer, Double> nodeTransferRdd = outDegrees.mapToPair(tuple -> 
				new Tuple2<Integer, Double>(tuple._1, 1.0 / tuple._2));
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = network.join(nodeTransferRdd);		
		JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Double>, Double>> joinedEdgePageRank = edgeTransferRDD.join(pageRankRdd);
		JavaPairRDD<Integer, Double> propagateRdd = joinedEdgePageRank.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._2._1._1, tuple._2._1._2 * tuple._2._2));
		pageRankRdd = propagateRdd.aggregateByKey(0.0,
				(val,row) -> val + row, (val,val2) -> val+val2);
		pageRankRdd = pageRankRdd.mapToPair(tuple -> new Tuple2<Integer, Double>(tuple._1, .15 + .85 * tuple._2));
		return pageRankRdd;
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
	}
	
	static double dmax;
	static int imax;
	static boolean debug;

	public static void main(String[] args) {
		final ComputeRanks cr = new ComputeRanks();
		try {
			cr.initialize();

			cr.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}
//		String usersTableName = "users";
//
//	    ArrayList<String> tempList = new ArrayList<String>();
//
//	    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();
//
//	    ScanRequest scanRequest = new ScanRequest()
//	        .withTableName(usersTableName);
//	    ScanResult result = dynamoDBClient.scan(scanRequest);
//
//	    for (Map<String, AttributeValue> item : result.getItems()) {
//	        tempList.add(item.toString());
//	        System.out.println(item.toString());
//	    }
//	    
//	    
//	    JavaSparkContext sc = SparkConnector.getSparkContext();
//	    JavaRDD<String> userRDD = JavaSparkContext.parallelize(tempList);
	    
	    
	    /*
	    String likesTableName = "newslikes";

	    ArrayList<String> likesList = new ArrayList<String>();

	    ScanRequest likesScanRequest = new ScanRequest()
	        .withTableName(likesTableName);
	    ScanResult likesResult = dynamoDBClient.scan(likesScanRequest);

	    for (Map<String, AttributeValue> item : likesResult.getItems()) {
	    	likesList.add(item.toString());
	        System.out.println(item.get("url").getS());
	        
	    }*/
	    
	        
	    /*String newsTableName = "news";

	    ArrayList<String> newsList = new ArrayList<String>();

	    ScanRequest newsScanRequest = new ScanRequest()
	        .withTableName(newsTableName);
	    ScanResult newsResult = dynamoDBClient.scan(newsScanRequest);

	    for (Map<String, AttributeValue> item : newsResult.getItems()) {
	    	newsList.add(item.toString());
	        System.out.println(item.toString());
	    }*/
	    
	    /*String friendsTableName = "friends";

	    ArrayList<String> friendsList = new ArrayList<String>();

	    ScanRequest friendsScanRequest = new ScanRequest()
	        .withTableName(friendsTableName);
	    ScanResult friendsResult = dynamoDBClient.scan(friendsScanRequest);

	    List<String> users = new ArrayList<String>();
	    for (Map<String, AttributeValue> item : result.getItems()) {
	    	friendsList.add(item.toString());
	        // System.out.println(item.toString());
	        System.out.println(item.get("username").getS());
	        users.add(item.get("username").getS());
	    }*/
	    
	    // JavaRDD<String> usersRDD = context.parallelize(users);
	        
	    // <ArticleNodes, <Usernodes, adsorption weight>>
	    
	    // JavaSparkContext sc = new JavaSparkContext();

	    // JavaPairRDD<Node, Node> rdd = sc.parallelize(friendsResult);
	    
	    // JavaPairRDD<Node,Tuple2<Node, Double>> graph;
	    
	        
	        /*
		final ComputeRanks cr = new ComputeRanks();
		debug = false;
		dmax = -1.0;
		imax = -1;

		if (args.length >= 1) {
			dmax = Double.valueOf(args[0]);
		}
		
		if (args.length >= 2) {
			imax = Integer.valueOf(args[1]);
		}
		
		if (args.length >= 3) {
			debug = true;
		}
		
		try {
			cr.initialize();

			cr.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}*/
	}

}
