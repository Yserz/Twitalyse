/*
 * Copyright (C) 2012 MacYser
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
package de.fhb.twitalyse.utils;

import java.io.Serializable;

/**
 *
 * @author MacYser
 */
public class Point implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -89003963673310945L;

	@Override
	public String toString() {
		return "Point [lat=" + lat + ", lng=" + lng + "]";
	}

	double lat;
	double lng;

	public Point(double _lat, double _lng) {
		lat = _lat;
		lng = _lng;
	}
}
