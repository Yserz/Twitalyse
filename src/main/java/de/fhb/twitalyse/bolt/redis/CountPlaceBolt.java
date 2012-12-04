package de.fhb.twitalyse.bolt.redis;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Counts Place names
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 *
 */
public class CountPlaceBolt extends BaseRedisBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7169731951340729319L;

	public CountPlaceBolt(String host, int port) {
		super(host, port);
		System.out.println();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		String place = input.getString(1);
		
		this.zincrBy("places", 1, place);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
