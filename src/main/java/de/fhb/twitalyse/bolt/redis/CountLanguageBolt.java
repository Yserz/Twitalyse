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

public class CountLanguageBolt extends BaseRedisBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8511876250962000527L;

	public CountLanguageBolt(String host, int port) {
		super(host, port);
	}

	@Override
	public void execute(Tuple input) {
		Long id = input.getLong(0);
		String language = input.getString(1);
		System.out.println("CountLanguageBolt Language: " + language);

		this.zincrby("languages", 1d, language);
		this.collector.ack(input);

	}
}
