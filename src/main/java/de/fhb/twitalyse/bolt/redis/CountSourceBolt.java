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
package de.fhb.twitalyse.bolt.redis;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
public class CountSourceBolt extends BaseRedisBolt {

	/**
	 *
	 */
	private static final long serialVersionUID = 8367958498374053860L;

	public CountSourceBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
			String source = input.getString(1);

			this.zincrby("sources", 1d, source);
			this.collector.emit(input, new Values(input.getLong(0)));
			this.collector.ack(input);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id"));
	}
}
