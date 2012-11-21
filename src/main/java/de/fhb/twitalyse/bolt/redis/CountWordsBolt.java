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

import de.fhb.twitalyse.bolt.statustext.*;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * This Bolt gets the Twitter Status Text out of the whole Status.
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class CountWordsBolt extends BaseRichBolt {

	private Jedis jedis;

	public CountWordsBolt(Jedis jedis) {
		this.jedis = jedis;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		System.out.println("CountWordsBolt Word: " + word);


		try {
			jedis.hincrBy("words", word, 1L);
//			long value = Long.valueOf(jedis.hget("words", word));
		} catch (JedisConnectionException e) {
			System.out.println("################################################");
			System.out.println("Exception: " + e);
			System.out.println("################################################");
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("id", "text"));
	}
}
