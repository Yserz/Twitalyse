package de.fhb.twitalyse;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import de.fhb.twitalyse.utils.CalcCoordinates;
import de.fhb.twitalyse.utils.Point;
public class CalcCoordinatesTest {

	
	public static final double EARTH_RADIUS_KM = 6372.8;
	
	private double radius = 60;
	
	private Point berlin = new Point(52.520399, 13.416264);
	private Point brandenburg = new Point(52.409033, 12.563337);
	
	@Before
	public void setup(){
		radius = 60;
		
		berlin = new Point(52.520399, 13.416264);
		brandenburg = new Point(52.409033, 12.563337);
	}
	
	@Test
	public void test() {
		Point notInRange = new Point(10, 10);
		System.out.println("distance in km: "+ CalcCoordinates.distanceInKm(berlin, brandenburg));
		assertTrue(CalcCoordinates.isPointInCircle(berlin, brandenburg, radius));
		assertFalse(CalcCoordinates.isPointInCircle(berlin, notInRange, radius));
	}
}
