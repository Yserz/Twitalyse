package de.fhb.twitalyse.bolt.redis;

import java.util.Map;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Some Redis Operations
 *
 * @author Christoph Ott <ott@fh-brandenburg.de>
 *
 */
public abstract class BaseRedisBolt extends BaseRichBolt {
	private final static Logger LOGGER = Logger.getLogger(BaseRedisBolt.class.getName());

	protected OutputCollector collector;
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
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2}", 
					new Object[]{e, e.getMessage(), e.getCause()});
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
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2}", 
					new Object[]{e, e.getMessage(), e.getCause()});
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
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2}", 
					new Object[]{e, e.getMessage(), e.getCause()});
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
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2}", 
					new Object[]{e, e.getMessage(), e.getCause()});
		}
	}

	@Override
	public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			jedis = new Jedis(host, port);
			jedis.getClient().setTimeout(9999);
		} catch (JedisException e) {
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2}", 
					new Object[]{e, e.getMessage(), e.getCause()});
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
		try {
			jedis.disconnect();
		} catch (JedisException e) {
			LOGGER.log(Level.SEVERE,"Exception: {0},\nMessage: {1},\nCause: {2}", 
					new Object[]{e, e.getMessage(), e.getCause()});
		}
	}
}
