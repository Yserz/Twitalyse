package de.fhb.twitalyse.bolt.redis;

import backtype.storm.tuple.Tuple;

/**
 * Counts Place names
 *
 * @author Christoph Ott <ott@fh-brandenburg.de>
 *
 */
public class CountPlaceBolt extends BaseRedisBolt {

	/**
	 *
	 */
	private static final long serialVersionUID = -7169731951340729319L;

	public CountPlaceBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		String place = input.getString(1);

		this.zincrby("places", 1d, place);
	}
}
