package de.fhb.twitalyse;

import de.fhb.twitalyse.utils.CalcCoordinates;
import de.fhb.twitalyse.utils.Point;
import org.junit.Test;

public class CoordTest {

	
	public static final double EARTH_RADIUS_KM = 6372.8;
	
	double latBerlin = 52.520399;
	double lngBerlin = 13.416264;
	
	double latBrand = 52.409033;
	double lngBrand = 12.563337;
	
	double radius = 60;
	
	Point berlin = new Point(latBerlin, lngBerlin);
	Point brandenburg = new Point(latBrand, lngBrand);
	
	
	@Test
	public void test() {
		CalcCoordinates calculator = new CalcCoordinates();
		System.out.println("is point in circle: "+ calculator.isPointInCircle(berlin, brandenburg, radius));
		System.out.println("distance in km: "+ calculator.distanceInKm(berlin, brandenburg));
	}
	

}
