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

/**
 *
 * @author Christoph Ott <ott@fh-brandenburg.de>
 */
public class CalcCoordinates {

	public static final double EARTH_RADIUS_KM = 6372.8;
		
	public static boolean isPointInCircle(Point p1, Point p2, double radius){
		if(distanceInKm(p1, p2) <= radius){
			return true;
		}else{
			return false;
		}
	}

	public static double distanceInKm(double lat1, double lon1, double lat2, double lon2) {
		double dLat = Math.toRadians(lat2 - lat1);
		double dLon = Math.toRadians(lon2 - lon1);
		lat1 = Math.toRadians(lat1);
		lat2 = Math.toRadians(lat2);

		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2)
				* Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
		double c = 2 * Math.asin(Math.sqrt(a));
		return EARTH_RADIUS_KM * c;
	}
	
	public static double distanceInKm(Point p1, Point p2) {
		return distanceInKm(p1.lat, p1.lng, p2.lat, p2.lng);
	}
}
