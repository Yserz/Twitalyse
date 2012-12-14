package de.fhb.twitalyse.bolt.redis;

import java.text.SimpleDateFormat;
import java.util.Date;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Christoph Ott <ott@fh-brandenburg.de>
 * 
 */
public class CountWordsInCircleBolt extends BaseRedisBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5197977666638999798L;

	public CountWordsInCircleBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(1);

		Date today = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy");

		// Saves all words of today
		this.zincrby("coordswords_" + sdf.format(today), 1d, word);
		// Saves all words
		this.zincrby("coordswords", 1d, word);
		// Saves # of filtered words
		this.incr("#coordswords_filtered");
		// Saves # of filtered words of today
		this.incr("#coordswords_filtered_" + sdf.format(today));

		this.collector.emit(input, new Values(input.getLong(0)));
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id"));
	}

}
