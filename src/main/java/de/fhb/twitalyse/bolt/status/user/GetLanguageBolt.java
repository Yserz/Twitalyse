/*
 * Copyright (C) 2012 Andy Klay
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
package de.fhb.twitalyse.bolt.status.user;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import de.fhb.twitalyse.bolt.data.Status;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This Bolt gets the Twitter Language Text out of the whole Status.
 *
 * @author Andy Klay <klay@fh-brandenburg.de>
 */
public class GetLanguageBolt extends BaseRichBolt {
	private final static Logger LOGGER = Logger.getLogger(GetLanguageBolt.class.getName());
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		LOGGER.log(Level.INFO, "GetLanguageBolt Status ID: {0}", id);
		String json = input.getString(1);

		try {
			Gson gson = new Gson();
			Status ts = gson.fromJson(json, Status.class);

			LOGGER.log(Level.INFO, "GetLanguageBolt Extracted Status Language: {0}", ts.user.lang);

			collector.emit(input, new Values(id, ts.user.lang));
			collector.ack(input);
		} catch (RuntimeException re) {
			LOGGER.log(Level.SEVERE, "Exception: {0},\nMessage: {1},\nCause: {2},\nJSON: {3}", 
					new Object[]{re, re.getMessage(), re.getCause(), json});
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "language"));
	}
}
