package de.fhb.twitalyse.bolt.status.coords;

import java.util.Map;

import de.fhb.twitalyse.utils.CalcCoordinates;
import de.fhb.twitalyse.utils.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Christoph Ott <ott@fh-brandenburg.de>
 *
 */
public class FilterCoordsBolt extends BaseRichBolt{

	private OutputCollector collector;
	private Point centerPoint;
	private double radius;
	
	public FilterCoordsBolt(Point centerPoint, double radius){
		this.centerPoint = centerPoint;
		this.radius = radius;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Point p = new Point(input.getFloat(1), input.getFloat(2));
		System.out.println("FilterCoords");
		System.out.println(p);
		System.out.println(centerPoint);
		System.out.println(radius);
		if (CalcCoordinates.isPointInCircle(centerPoint, p, radius)) {
			System.out.println("is in circle");
			this.collector.emit(input, new Values(input.getLong(0), input.getString(3)));
			this.collector.ack(input);
		} else {
			System.out.println("is in NOT CIRCLE");
			this.collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "text"));
	}

}
