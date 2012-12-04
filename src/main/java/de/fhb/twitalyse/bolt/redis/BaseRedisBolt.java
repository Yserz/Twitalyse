package de.fhb.twitalyse.bolt.redis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import backtype.storm.topology.base.BaseRichBolt;
import java.util.Map;

/**
 * Some Redis Operations
 * 
 * @author Christoph Ott <ott@fh-brandenburg.de>
 * 
 */
public abstract class BaseRedisBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3005326024690433763L;

	protected String host;

	protected int port;

	protected BaseRedisBolt(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}

	/**
	 * @see Jedis#zincrby(String, double, String)
	 * 
	 * @param key
	 * @param score
	 * @param member
	 */
	protected void zincrby(String key, Double score, String member) {
		try {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			jedis.zincrby(key, score, member);
			jedis.disconnect();
		} catch (JedisConnectionException e) {
			System.out.println("Exception: " + e);
		}
	}

	/**
	 * @see Jedis#incr(String)
	 * 
	 * @param key
	 */
	protected void incr(String key) {
		try {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			jedis.incr(key);
			jedis.disconnect();
		} catch (JedisConnectionException e) {
			System.out.println("Exception: " + e);
		}
	}
	/**
	 * @see Jedis#incrBy(String, long)
	 * 
	 * @param key
	 * @param integer
	 */
	protected void incrBy(String key, long integer) {
		try {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			jedis.incrBy(key, integer);
			jedis.disconnect();
		} catch (JedisConnectionException e) {
			System.out.println("Exception: " + e);
		}
	}

	/**
	 * @see Jedis#hincrBy(String, String, long)
	 * 
	 * @param key
	 * @param integer
	 */
	protected void hincrBy(String key, String field, long value) {
		try {
			Jedis jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
			jedis.hincrBy(key, field, value);
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
