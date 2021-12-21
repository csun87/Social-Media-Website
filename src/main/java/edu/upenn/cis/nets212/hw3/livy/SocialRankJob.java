package edu.upenn.cis.nets212.hw3.livy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import edu.upenn.cis.nets212.storage.SparkConnector;
import scala.Tuple2;

class DynamoConnector {
	/**
	 * This is our connection
	 */
	static DynamoDB client;

	/**
	 * Singleton pattern: get the client connection if one exists, else create one
	 * 
	 * @param url
	 * @return
	 */
	public static DynamoDB getConnection(final String url) {
		if (client != null)
			return client;
		
	    	client = new DynamoDB( 
	    			AmazonDynamoDBClientBuilder.standard()
					.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
						"https://dynamodb.us-east-1.amazonaws.com", "us-east-1"))
        			.withCredentials(new DefaultAWSCredentialsProviderChain())
					.build());

    	return client;
	}
	
	/**
	 * Orderly shutdown
	 */
	public static void shutdown() {
		if (client != null) {
			client.shutdown();
			client = null;
		}
		System.out.println("Shut down DynamoDB factory");
	}
}

public class SocialRankJob implements Job<List<MyPair<Integer,Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	DynamoDB db;
	
	Table table;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		db = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		System.out.println("Initializing tables...");
		initializeTables();
		System.out.println("Connected!");
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<MyPair<Integer,Double>> run() throws IOException, InterruptedException {
		//Scan table for friends: gets user nodes and (u, u) edges
				System.out.println("Running...");
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
			    
			    
			    
			    
			    
			    Map<String, AttributeValue> expressionAttributeValues =
				new HashMap<String, AttributeValue>();
				expressionAttributeValues.put(":date", new AttributeValue().withS("2017-12-21"));
				Map<String, String> expressionAttributeNames =
				new HashMap<String, String>();
				expressionAttributeNames.put("#d", "date"); 
			    //Scan table for newslikes: add edges b/w users, urls
			    String likesTableName = "newslikes";
			    List<Tuple2<String, String>> likesList = new ArrayList<>();
			    //List<Tuple2<String, String>> articleCategoryList = new ArrayList<>();
			    ScanRequest likesScanRequest = new ScanRequest().withTableName(likesTableName); 
			    ScanResult likesResult = dynamoDBClient.scan(likesScanRequest);
			    //scan table for (username, liked article)
			    for (Map<String, AttributeValue> item : likesResult.getItems()) {
			    	likesList.add(new Tuple2<String, String>(item.get("userid").getS(), item.get("url").getS()));
//			    	articleCategoryList.add(new Tuple2<String, String>(item.get("category").getS(),
//			    			item.get("url").getS()));
			    }
			    JavaRDD<Tuple2<String, String>> likeEdges = context.parallelize(likesList);
			    JavaPairRDD<String, String> likeEdgesRDD = likeEdges.mapToPair(x -> 
			    new Tuple2<String, String>(x._1, x._2));
			    JavaPairRDD<String, Integer> numLikesRDD = likeEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
			    JavaPairRDD<String, Double> likesPropRDD = numLikesRDD.mapValues(i -> (double)(0.4)/i);
			    //edges between (u, a) = (u, (a, scale factor))
			    JavaPairRDD<String, Tuple2<String, Double>> uaEdgeWeightRDD = likeEdgesRDD.join(likesPropRDD);
//			    for (Tuple2<String, Tuple2<String, Double>> t: uaEdgeWeightRDD.collect()) {
//			    	System.out.println(t._1 + "->" + t._2._1 + "\t" + t._2._2);
//			    }
			    
			    JavaPairRDD<String, String> likedEdgesRDD= likeEdges.mapToPair(x -> 
			    	new Tuple2<String, String>(x._2, x._1));
			    JavaPairRDD<String, Integer> numLikedRDD = likedEdgesRDD.mapValues(x->1).reduceByKey((x, y) -> (x+y));
			    JavaPairRDD<String, Double> likedPropRDD = numLikedRDD.mapValues(i -> (double)(0.4)/i);
			    //edges between (a, u) = (a, (u, scale factor))
			    JavaPairRDD<String, Tuple2<String, Double>> auEdgeWeightRDD = likedEdgesRDD.join(likedPropRDD);
			    
			    
//			    JavaRDD<Tuple2<String, String>> articleCategoryEdges = context.parallelize(articleCategoryList);
//			    //for edge a.1 -> a.2 or user -> url, make (url, user) or (a.2, a.1)
//			    JavaPairRDD<String, String> acEdgesRDD = articleCategoryEdges.mapToPair(
//			    		x -> new Tuple2<String, String>(x._2, x._1));
//			    JavaPairRDD<String, Integer> numACEdges = acEdgesRDD.mapValues(x -> 1).reduceByKey((x, y) -> (x+y));
			    
			    
			    
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
			    String newsTableName = "news";
				List<Tuple2<String, String>> articleCategoryList = new ArrayList<>();
//				ScanRequest newsScanRequest = new ScanRequest().withTableName(newsTableName)
//				.withFilterExpression("#d = :date").withExpressionAttributeValues(expressionAttributeValues)
//				.withExpressionAttributeNames(expressionAttributeNames);
//				ScanResult newsResult = dynamoDBClient.scan(newsScanRequest);
				Map<String, AttributeValue> lastKey = null;
				do {
					ScanRequest newsScanRequest = new ScanRequest().withTableName(newsTableName)
							.withFilterExpression("#d = :date").withExpressionAttributeValues(expressionAttributeValues)
							.withExpressionAttributeNames(expressionAttributeNames)
							.withExclusiveStartKey(lastKey);
					ScanResult newsResult = dynamoDBClient.scan(newsScanRequest);
					List<Map<String, AttributeValue>> items = newsResult.getItems();
					Iterator<Map<String, AttributeValue>> iter = items.iterator();
					while (iter.hasNext()) {
						Map<String, AttributeValue> item = iter.next();
						if (item != null) {
				    		if (item.containsKey("headline")) {
				    			System.out.println(item.get("headline").getS());
				    		}
						}
						if (item.containsKey("url") && item.containsKey("category")) {
							articleCategoryList.add(new Tuple2<>(item.get("url").getS(), item.get("category").getS()));
						}
					}
					lastKey = newsResult.getLastEvaluatedKey();
				} while (lastKey != null);
//				for (Map<String, AttributeValue> item : newsResult.getItems()){
//				    	if (item != null) {
//				    		if (item.containsKey("headline")) {
//							System.out.println(item.get("headline").getS());
//						}
//						if (item.containsKey("url") && item.containsKey("category")) {
//							articleCategoryList.add(new Tuple2<>(item.get("url").getS(), item.get("category").getS()));
//						}
//					}
//				}
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
			    JavaPairRDD<String, Tuple2<Tuple2<String, Double>, String>> temp = nodeRDD.join(articleCategoryRDD);
			    nodeRDD = temp.mapToPair(x -> new Tuple2<>(x._1, new Tuple2<>(x._2._1._1, x._2._1._2))); 
			    System.out.println("Writing to database");
			    writeToTable(nodeRDD);
			    List<MyPair<Integer, Double>> out = new ArrayList<>();
			    return out;
	}
	
	private void initializeTables() throws InterruptedException {
		try {
			
			table = db.createTable("adsorption", Arrays.asList(new KeySchemaElement("username", KeyType.HASH), // Partition key
							new KeySchemaElement("url", KeyType.RANGE)), Arrays.asList(new AttributeDefinition("username", ScalarAttributeType.S),
									new AttributeDefinition("url", ScalarAttributeType.S)),
							new ProvisionedThroughput(25L, 25L));
			table.waitForActive();
		} catch (final ResourceInUseException exists) {
			table = db.getTable("adsorption");
		}
	}
	
	void writeToTable(JavaPairRDD<String, Tuple2<String, Double>> data) {
		data.foreachPartition((partition) -> {
			DynamoDB docClient = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
			TableWriteItems tbItems = new TableWriteItems("adsorption");
			Set<String> used = new HashSet<>();
			short cnt = 0;
			while (partition.hasNext()) {
				Tuple2<String, Tuple2<String, Double>> x = partition.next();
				String url = x._1;
				String username = x._2._1;
				if (used.add(username + "$" + url)) {
					Double weight = x._2._2;
					Item item = new Item().withPrimaryKey("username", username)
							.withString("url", url)
							.withNumber("weight", weight);
					tbItems.addItemToPut(item);
					++cnt;
					if (cnt == 25) {
						BatchWriteItemOutcome outcome = docClient.batchWriteItem(tbItems);
						while (outcome.getUnprocessedItems().size() > 0) {
							outcome = docClient.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
						}
						cnt = 0;
						tbItems = new TableWriteItems("adsorption");
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
	 * Graceful shutdown
	 */
	public void shutdown() {
		System.out.println("Shutting down");
	}
	
	public SocialRankJob() {
		System.setProperty("file.encoding", "UTF-8");
	}

	@Override
	public List<MyPair<Integer,Double>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
