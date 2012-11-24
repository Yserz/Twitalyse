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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 *
 * @author "ott"
 */
public class CountSourceBolt extends BaseRichBolt {

	private String host;
	private int port;

	public CountSourceBolt(String host, int port) {
		this.host = host;
		this.port = port;
	}

	

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void execute(Tuple input) {
		String source = input.getString(1);
		System.out.println("CountSourceBolt Word: " + source);


		try {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			
			jedis.hincrBy("sources", source, 1L);
			
			jedis.disconnect();
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
