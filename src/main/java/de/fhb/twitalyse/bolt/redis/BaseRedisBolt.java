package de.fhb.twitalyse.bolt.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import backtype.storm.topology.base.BaseRichBolt;

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
	protected void zincrBy(String key, double score, String member) {
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
}
