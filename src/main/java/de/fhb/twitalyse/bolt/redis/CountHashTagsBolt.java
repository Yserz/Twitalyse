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

import java.text.SimpleDateFormat;
import java.util.Date;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This Bolt gets the Twitter Status Text out of the whole Status.
 * 
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class CountHashTagsBolt extends BaseRedisBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3012129841702694630L;

	public CountHashTagsBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
		String hashtag = input.getString(1);

		Date today = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy");

		this.zincrby("hashtags_" + sdf.format(today), 1d, hashtag);
		this.zincrby("hashtags", 1d, hashtag);

		this.collector.emit(input, new Values(input.getLong(0)));
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id"));
	}
}
