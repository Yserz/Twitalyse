package de.fhb.twitalyse;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.fhb.twitalyse.bolt.redis.CountWordsInCircleBolt;
import de.fhb.twitalyse.bolt.status.coords.FilterCoordsBolt;
import de.fhb.twitalyse.bolt.status.coords.GetCoordsBolt;
import de.fhb.twitalyse.bolt.status.text.SplitStatusTextBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;
import de.fhb.twitalyse.utils.Point;

/**
 * This Topology analyses Twitter Stati posted on the Twitter Public Channel.
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
@Deprecated
public class LangCoordsInCircleTopology {
	private final static Logger LOGGER = Logger.getLogger(LangCoordsInCircleTopology.class.getName());

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
		initLogger();
		builder = new TopologyBuilder();
		initTwitterSpout();
		initGetCoordsForLang();
	}

	private void initLogger() {
		Level consoleHandlerLevel = Level.SEVERE;
		Level fileHandlerLevel = Level.INFO;
		Date today = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy");

		//setting up ConsoleHandler
		Logger rootLogger = Logger.getLogger("");

		Handler[] handlers = rootLogger.getHandlers();

		ConsoleHandler chandler = null;

		for (int i = 0; i < handlers.length; i++) {
			if (handlers[i] instanceof ConsoleHandler) {
				chandler = (ConsoleHandler) handlers[i];
			}
		}

		if (chandler != null) {
			chandler.setLevel(consoleHandlerLevel);
		} else {
			LOGGER.log(Level.SEVERE, "No ConsoleHandler there.");
		}

		//setting up FileHandler
		FileHandler fh = null;
		try {
			fh = new FileHandler("log/log_" + sdf.format(today) + ".log");
			fh.setFormatter(new SimpleFormatter());
			fh.setLevel(fileHandlerLevel);
		} catch (IOException ex) {
			new File("log").mkdir();
			try {
				fh = new FileHandler("log/log_" + sdf.format(today) + ".log");
				fh.setFormatter(new SimpleFormatter());
				fh.setLevel(fileHandlerLevel);
			} catch (IOException ex1) {
				System.err.println("Input-output-error while creating the initial log.");
				LOGGER.log(Level.SEVERE, null, ex1);
			} catch (SecurityException ex1) {
				LOGGER.log(Level.SEVERE, null, ex1);
			}
			LOGGER.log(Level.SEVERE, null, ex);

		} catch (SecurityException ex) {
			System.err.println("Cannot open/access Log-Folder so I will not log anything.");
			LOGGER.log(Level.SEVERE, null, ex);
		}

		if (fh != null) {
			rootLogger.addHandler(fh);
		}
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
				"stopWords.properties").getProperty("ignoreWords");
		ignoreList = Arrays.asList(ignoreWords.split(";"));

		Properties redisProps = propLoader
				.loadSystemProperty("redisProps.properties");
		redisHost = redisProps.getProperty("host");
		redisPort = Integer.valueOf(redisProps.getProperty("port"));
	}

	private void initGetCoordsForLang() {
		GetCoordsBolt coords = new GetCoordsBolt();
		FilterCoordsBolt filterCoords = new FilterCoordsBolt(centerPoint, radius, redisHost, redisPort);
		CountWordsInCircleBolt count = new CountWordsInCircleBolt(redisHost, redisPort);
		SplitStatusTextBolt splitText = new SplitStatusTextBolt(ignoreList, redisHost, redisPort);

		builder.setBolt("coords", coords, 4)
				.shuffleGrouping(TWITTERSPOUT);
		builder.setBolt("filterCoords", filterCoords).shuffleGrouping("coords");
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
		}else if(args.length == 4){
			this.centerPoint = new Point(Double.parseDouble(args[1]) , Double.parseDouble(args[2]));
			this.radius = Double.parseDouble(args[3]);
			
			initBuilder();
			
			conf.setMaxTaskParallelism(8);
			conf.setNumWorkers(8);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		}else{
			LOGGER.log(Level.SEVERE, "Wrong Number of args\n"
					+ "<UI_NAME> <latitude> <longitude> <radius>\n"
					+ "Topology not startet");
		}
	}

	public static void main(String[] args) throws IOException {
		LangCoordsInCircleTopology a = new LangCoordsInCircleTopology();
		try {
			a.startTopology(args);
		} catch (AlreadyAliveException e) {
			LOGGER.log(Level.SEVERE, e+"\n"+e.getMessage());
		} catch (InvalidTopologyException e) {
			LOGGER.log(Level.SEVERE, e+"\n"+e.getMessage());
		} catch (InterruptedException e) {
			LOGGER.log(Level.SEVERE, e+"\n"+e.getMessage());
		}
	}
}
