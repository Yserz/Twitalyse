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
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class AnalyseStatusTextBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		String json = input.getString(1);
		Gson gson = new Gson();
		Status ts = gson.fromJson(json, Status.class);
		// TODO: Split and Wordcount here
//		collector.emit(input, new Values(val * 2, val * 3));
		collector.ack(input);
	}

//	@Override
//	public void cleanup() {
//		
//	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("double", "triple"));
	}
}
