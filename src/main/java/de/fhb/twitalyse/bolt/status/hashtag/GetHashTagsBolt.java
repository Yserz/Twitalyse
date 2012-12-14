package de.fhb.twitalyse.bolt.status.hashtag;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;

import de.fhb.twitalyse.bolt.data.Hashtag;
import de.fhb.twitalyse.bolt.data.Status;

/**
 * This Bolt gets the Twitter Status Source out of the whole Status.
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
public class GetHashTagsBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3043768502053640614L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Long id = input.getLong(0);
		String json = input.getString(1);

		Gson gson = new Gson();
		Status ts = gson.fromJson(json, Status.class);

		for (Hashtag hashtag : ts.entities.hashtags) {
			collector.emit(input, new Values(id, hashtag.text));
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "hashtag"));
	}
}
