package de.fhb.twitalyse.bolt.status.coords;

import java.util.Map;

import com.google.gson.Gson;

import de.fhb.twitalyse.bolt.Status;
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
public class GetCoordsForLangBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private String lang;
	
	public GetCoordsForLangBolt(String lang){
		this.lang = lang;
	}

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
			if(ts.coordinates != null){
			System.out.println("LAT:" +ts.coordinates.coordinates.get(1));
			System.out.println("LNG:" +ts.coordinates.coordinates.get(0));
			System.out.println(CalcCoordinates.distanceInKm(new Point(ts.coordinates.coordinates.get(1), ts.coordinates.coordinates.get(0)), new Point(40.044438, -98.187011)));
			System.out.println("-----------------------------------------");
			}
			
			if(ts.user.lang == lang && ts.coordinates != null){
				System.out.println("OK");
				collector.emit(input, new Values(id, ts.coordinates.coordinates.get(1), ts.coordinates.coordinates.get(0), ts.text));
				collector.ack(input);
			}else if(ts.coordinates != null){
				System.out.println(ts.user.lang);
				collector.emit(input, new Values(id, ts.coordinates.coordinates.get(1), ts.coordinates.coordinates.get(0), ts.text));
				collector.ack(input);
			}else{
				collector.ack(input);
			}
		} catch (RuntimeException re) {
			System.out
					.println("########################################################");
			System.out.println(re + "\n" + re.getMessage());
			System.out.println("JSON: " + json);
			System.out
					.println("########################################################");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "lat", "lng", "text"));
	}
}
