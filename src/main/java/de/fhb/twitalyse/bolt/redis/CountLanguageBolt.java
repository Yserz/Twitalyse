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
package de.fhb.twitalyse.bolt.redis;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.mortbay.log.Log;

public class CountLanguageBolt extends BaseRedisBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8511876250962000527L;

	public CountLanguageBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
		try {

			Long id = input.getLong(0);
			String language = input.getString(1);
//			Log.info("CountLanguageBolt Language: " + language);

			this.zincrby("languages", 1d, language);
			
			this.collector.emit(input, new Values(input.getLong(0)));
			this.collector.ack(input);

		} catch (Exception e) {
			Log.warn(e);
			this.collector.fail(input);
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id"));
	}
}
