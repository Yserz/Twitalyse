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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * This Bolt adds the Twitter Hashtag Count into Redis.
 * 
 * @author Andy Klay <koppen@fh-brandenburg.de>
 */
public class CountHashtagBolt extends BaseRichBolt {

	private String host;
	private int port;

	public CountHashtagBolt(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		String language = input.getString(1);
		System.out.println("CountLanguageBolt Language: " + language);

		try {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			
			jedis.zincrby("languages", 1, language);
			
			jedis.disconnect();
		} catch (JedisConnectionException e) {
			System.out.println("Exception: " + e);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}

}
