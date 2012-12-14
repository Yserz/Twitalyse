package de.fhb.twitalyse.bolt.status.source;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;

import de.fhb.twitalyse.bolt.data.Status;
import de.fhb.twitalyse.utils.TwitterUtils;
import org.mortbay.log.Log;

/**
 * This Bolt gets the Twitter Status Source out of the whole Status.
 *
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
public class GetStatusSourceBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Long id = input.getLong(0);
//		Log.info("GetStatusSourceBolt Status ID: {0}", id);
		String json = input.getString(1);

		try {
			Gson gson = new Gson();
			Status ts = gson.fromJson(json, Status.class);

//			Log.info("GetStatusSourceBolt Extracted Source Text: {0}", ts.source);
			
			String source = TwitterUtils.findSource(ts.source);

			collector.emit(input, new Values(id, source));
			collector.ack(input);
		
		}catch (Exception re) {
			Log.warn("Exception: {0},\nMessage: {1},\nCause: {2},\nJSON: {3}", 
					new Object[]{re, re.getMessage(), re.getCause(), json});
			collector.fail(input);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "text"));
	}
}
