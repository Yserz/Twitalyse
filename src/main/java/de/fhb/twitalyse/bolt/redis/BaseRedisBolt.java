package de.fhb.twitalyse.bolt.redis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import redis.clients.jedis.Jedis;
import backtype.storm.topology.base.BaseRichBolt;
import java.util.Map;
import redis.clients.jedis.exceptions.JedisException;

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
	private String host;
	private int port;
	private transient Jedis jedis;

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
	protected void zincrby(String key, double score, String member) {
		try {
			jedis.zincrby(key, score, member);
		} catch (JedisException e) {
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
			jedis.incr(key);
		} catch (JedisException e) {
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
			jedis.incrBy(key, integer);
		} catch (JedisException e) {
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
			jedis.hincrBy(key, field, value);
		} catch (JedisException e) {
			System.out.println("Exception: " + e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
		} catch (JedisException e) {
			System.out.println("Exception: " + e);
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
		try {
			jedis.disconnect();
		} catch (JedisException e) {
			System.out.println("Exception: " + e);
		}
	}
}
