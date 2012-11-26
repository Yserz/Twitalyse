package de.fhb.twitalyse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class loads various Property-Files.
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class PropertyLoader {

	/**
	 * load the SystemProperty
	 *
	 * @param path to property file
	 * @return Properties
	 * @throws IOException
	 */
	public Properties loadSystemProperty(String path) throws IOException {
		Properties props = new Properties();
		try {
			ClassLoader loader = PropertyLoader.class.getClassLoader();
			InputStream in = loader.getResourceAsStream(path);
			props.load(in);
			
		} finally {
			//May set some default values.
		}
		return props;
	}
}
