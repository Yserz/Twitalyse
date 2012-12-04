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
package de.fhb.twitalyse.bolt.status.text;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import de.fhb.twitalyse.bolt.Status;
import java.util.Map;

/**
 * This Bolt gets the Twitter Status Text out of the whole Status.
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class GetStatusTextBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Long id = input.getLong(0);
		System.out.println("GetStatusTextBolt Status ID: " + id);
		String json = input.getString(1);
//		System.out.println("GetStatusTextBolt JSON: "+json);

		try {
			Gson gson = new Gson();
			Status ts = gson.fromJson(json, Status.class);

			System.out.println("GetStatusTextBolt Extracted Status Text: " + ts.text);

			collector.emit(input, new Values(id, ts.text));
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
