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
package de.fhb.twitalyse;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import de.fhb.twitalyse.utils.PropertyLoader;

/**
 *
 * @author MacYser
 */
public class PropertyLoaderTest {
	

	/**
	 * Test of loadSystemProperty method, of class PropertyLoader.
	 */
	@Test
	public void testLoadSystemProperty() throws Exception {
		System.out.println("loadSystemProperty");
		
		String path = "twitterProps.properties";
		PropertyLoader instance = new PropertyLoader();
		Properties result = instance.loadSystemProperty(path);
		
		
		Set<Object> keys = result.keySet();
		for (Object key : keys) {
			System.out.println(key.toString() + " = " + result.get(key).toString());
		}
		
	}
}
