/*
 * Copyright (C) 2012 Michael Koppen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fhb.twitalyse.bolt.statustext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import de.fhb.twitalyse.bolt.Status;
import java.util.Map;

/**
 * This Bolt gets the Twitter Status Source out of the whole Status.
 *
 * @author "ott"
 */
public class GetStatusSourceBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		System.out.println("GetStatusSourceBolt Status ID: " + id);
		String json = input.getString(1);

		try {
			Gson gson = new Gson();
			Status ts = gson.fromJson(json, Status.class);

			System.out.println("GetStatusSourceBolt Extracted Source Text: " + ts.source);

			collector.emit(input, new Values(id, ts.source));
			collector.ack(input);
		} catch (RuntimeException re) {
			System.out.println("########################################################");
			System.out.println("Exception: "+re);
			System.out.println("JSON: "+json);
			System.out.println("########################################################");
		}

	}

//	@Override
//	public void cleanup() {
//		
//	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "text"));
	}
}
