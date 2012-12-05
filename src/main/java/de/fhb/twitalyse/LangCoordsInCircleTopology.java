package de.fhb.twitalyse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.fhb.twitalyse.bolt.redis.CountLanguageBolt;
import de.fhb.twitalyse.bolt.redis.CountSourceBolt;
import de.fhb.twitalyse.bolt.redis.CountWordsBolt;
import de.fhb.twitalyse.bolt.redis.CountWordsInLangCoordsBolt;
import de.fhb.twitalyse.bolt.status.coords.FilterCoordsBolt;
import de.fhb.twitalyse.bolt.status.coords.GetCoordsForLangBolt;
import de.fhb.twitalyse.bolt.status.source.GetStatusSourceBolt;
import de.fhb.twitalyse.bolt.status.user.GetLanguageBolt;
import de.fhb.twitalyse.bolt.status.text.GetStatusTextBolt;
import de.fhb.twitalyse.bolt.status.text.SplitStatusTextBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;
import de.fhb.twitalyse.utils.Point;

/**
 * This Topology analyses Twitter Stati posted on the Twitter Public Channel.
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
public class LangCoordsInCircleTopology {

	private static final String TWITTERSPOUT = "twitterSpout";
	private TopologyBuilder builder;
	private String consumerKey;
	private String consumerKeySecure;
	private final String DEFAULT_LANG = "de";
	private final int BOLT_PARALLELISM = 3;
	private List<String> ignoreList;
	private String redisHost;
	private int redisPort;
	private String token;
	private String tokenSecret;
	private String lang;
	private Point centerPoint;
	private double radius;

	public LangCoordsInCircleTopology() throws IOException {
		initProperties();
	}

	private void initBuilder() {
		builder = new TopologyBuilder();
		initTwitterSpout();
		initGetCoordsForLang();
	}

	private void initProperties() throws IOException {
		PropertyLoader propLoader = new PropertyLoader();

		Properties twitterProps = propLoader
				.loadSystemProperty("twitterProps.properties");

		consumerKey = twitterProps.getProperty("consumerKey");
		consumerKeySecure = twitterProps.getProperty("consumerKeySecure");
		token = twitterProps.getProperty("token");
		tokenSecret = twitterProps.getProperty("tokenSecret");

		String ignoreWords = propLoader.loadSystemProperty(
				"ignoreWords.properties").getProperty("ignoreWords");
		ignoreList = Arrays.asList(ignoreWords.split(";"));

		Properties redisProps = propLoader
				.loadSystemProperty("redisProps.properties");
		redisHost = redisProps.getProperty("host");
		redisPort = Integer.valueOf(redisProps.getProperty("port"));
	}

	private void initGetCoordsForLang() {
		GetCoordsForLangBolt coordsLang = new GetCoordsForLangBolt(lang);
		FilterCoordsBolt filterCoords = new FilterCoordsBolt(centerPoint, radius, redisHost, redisPort);
		CountWordsInLangCoordsBolt count = new CountWordsInLangCoordsBolt(redisHost, redisPort);
		SplitStatusTextBolt splitText = new SplitStatusTextBolt(ignoreList, redisHost, redisPort);

		builder.setBolt("coordsLang", coordsLang, 4)
				.shuffleGrouping(TWITTERSPOUT);
		builder.setBolt("filterCoords", filterCoords).shuffleGrouping("coordsLang");
		builder.setBolt("splitText", splitText).shuffleGrouping("filterCoords");
		builder.setBolt("countWordsInLangCoords", count).shuffleGrouping("splitText");

	}

	private void initTwitterSpout() {
		TwitterStreamSpout twitterStreamSpout = new TwitterStreamSpout(
				consumerKey, consumerKeySecure, token, tokenSecret, redisHost,
				redisPort);
		builder.setSpout(TWITTERSPOUT, twitterStreamSpout, 1);
	}

	/**
	 * (args.length == 0) LocalCluster <br>
	 * args[0] - Name of Topology for Storm ui (String)<br>
	 * args[1] - Number of workers (int)
	 * 
	 * @param args
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws InterruptedException
	 */
	public void startTopology(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {
		Config conf = new Config();
		conf.setDebug(false);
		
		if (args == null || args.length == 0){
			conf.setMaxTaskParallelism(3);
			this.lang = "en";
			// New York
			//this.centerPoint = new Point(40.712134, -74.004988);
			
			//Mitte EU
			this.centerPoint = new Point(49.124219, 5.882080);


			this.radius = 10000;
			
			initBuilder();
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitalyse", conf, builder.createTopology());

			Thread.sleep(60000);

			cluster.shutdown();
		}else if(args.length == 5){
			this.lang = args[1];
			this.centerPoint = new Point(Double.parseDouble(args[2]) , Double.parseDouble(args[3]));
			this.radius = Double.parseDouble(args[4]);
			
			initBuilder();
			
			conf.setMaxTaskParallelism(8);
			conf.setNumWorkers(8);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		}else{
			System.out.println("Wrong Number of args");
			System.out.println("<UI_NAME> <language> <latitude> <longitude> <radius>");
			System.out.println("Topology not startet");
		}
	}

	public static void main(String[] args) throws IOException {
		LangCoordsInCircleTopology a = new LangCoordsInCircleTopology();
		try {
			a.startTopology(args);
		} catch (AlreadyAliveException e) {
			System.out.println(e+"\n"+e.getMessage());
		} catch (InvalidTopologyException e) {
			System.out.println(e+"\n"+e.getMessage());
		} catch (InterruptedException e) {
			System.out.println(e+"\n"+e.getMessage());
		}
	}
}
