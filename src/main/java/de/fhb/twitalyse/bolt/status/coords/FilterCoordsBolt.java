package de.fhb.twitalyse.bolt.status.coords;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.fhb.twitalyse.bolt.redis.BaseRedisBolt;
import de.fhb.twitalyse.utils.CalcCoordinates;
import de.fhb.twitalyse.utils.Point;
import org.mortbay.log.Log;

/**
 * @author Christoph Ott <ott@fh-brandenburg.de>
 * 
 */
public class FilterCoordsBolt extends BaseRedisBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6617639744490449642L;

	private Point centerPoint;
	private double radius;

	public FilterCoordsBolt(Point centerPoint, double radius, String host,
			int port) {
		super(host, port);
		this.centerPoint = centerPoint;
		this.radius = radius;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {

			Point p = new Point(input.getFloat(1), input.getFloat(2));

			if (CalcCoordinates.isPointInCircle(centerPoint, p, radius)) {
				this.incr("#stati_inCircle");
				this.collector.emit(input,
						new Values(input.getLong(0), input.getString(3)));
				this.collector.ack(input);
			} else {
//				Log.info("is NOT CIRCLE");
				this.collector.ack(input);
			}
		} catch (Exception e) {
			Log.warn(e);
			this.collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "text"));
	}

}
