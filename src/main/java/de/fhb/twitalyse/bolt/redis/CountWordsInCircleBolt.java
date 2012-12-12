package de.fhb.twitalyse.bolt.redis;

import java.text.SimpleDateFormat;
import java.util.Date;

import backtype.storm.tuple.Tuple;

/**
 * @author Christoph Ott <ott@fh-brandenburg.de>
 *
 */
public class CountWordsInCircleBolt extends BaseRedisBolt{

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
		
		this.collector.ack(input);
	}

}
