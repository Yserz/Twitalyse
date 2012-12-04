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

import backtype.storm.tuple.Tuple;

/**
 * This Bolt adds the Twitter Follower Count into Redis.
 *
 * @author Andy Klay <koppen@fh-brandenburg.de>
 */
public class CountFollowerBolt extends BaseRedisBolt {

	public CountFollowerBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLong(0);
		System.out.println("CountFollowerBolt Status ID: " + id);
		long counter = input.getLong(1);
		System.out.println("CountFollowerBolt : " + counter);

		this.incrBy("#follower", counter);


	}
}
