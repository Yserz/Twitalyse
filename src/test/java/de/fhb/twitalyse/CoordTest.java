package de.fhb.twitalyse;

import org.junit.Test;

public class CoordTest {

	
	public static final double EARTH_RADIUS_KM = 6372.8;
	
	double latBerlin = 52.520399;
	double lngBerlin = 13.416264;
	
	double latBrand = 52.409033;
	double lngBrand = 12.563337;;
	
	double radius = 200;
	
	Point berlin = new Point(latBerlin, lngBerlin);
	Point brandenburg = new Point(latBrand, lngBrand);
	
	public class Point{
		
		public Point(double _lat, double _lng){
			lat = _lat;
			lng = _lng;
		}
		
		double lat;
		double lng;
	}
	
	@Test
	public void test() {
		System.out.println(isPointInCircle(berlin, brandenburg, radius));
	}
	
	public boolean isPointInCircle(Point p1, Point p2, double radius){
		if(haversine(p1.lat, p1.lng, p2.lat, p2.lng)<=radius){
			return true;
		}else{
			return false;
		}
	}

	public double haversine(double lat1, double lon1, double lat2, double lon2) {
		double dLat = Math.toRadians(lat2 - lat1);
		double dLon = Math.toRadians(lon2 - lon1);
		lat1 = Math.toRadians(lat1);
		lat2 = Math.toRadians(lat2);

		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2)
				* Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
		double c = 2 * Math.asin(Math.sqrt(a));
		return EARTH_RADIUS_KM * c;
	}

}
