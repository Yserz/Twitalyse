package de.fhb.twitalyse.bolt.status.coords;

import java.util.Map;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;

import de.fhb.twitalyse.bolt.data.Status;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Christoph Ott <ott@fh-brandenburg.de>
 * 
 */
public class GetCoordsBolt extends BaseRichBolt {
	private final static Logger LOGGER = Logger.getLogger(GetCoordsBolt.class.getName());

	/**
	 *
	 */
	private static final long serialVersionUID = 2075295658799531985L;
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

		try {
			Gson gson = new Gson();
			Status ts = gson.fromJson(json, Status.class);

			if (ts.coordinates != null) {
				// lat is 1
				// lng is 0
				collector.emit(new Values(id,
						ts.coordinates.coordinates.get(1),
						ts.coordinates.coordinates.get(0), ts.text));

			}
			collector.ack(input);

		} catch (RuntimeException re) {
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2},\nJSON: {3}",
					new Object[] { re, re.getMessage(), re.getCause(), json });
			collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "lat", "lng", "text"));
	}
}
