package de.fhb.twitalyse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * Loads some propertyfiles
 *
 * @author Michael Koppen mail: koppen@fh-brandenburg.de
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
			URL url = de.fhb.twitalyse.PropertyLoader.class.getResource(path);
			File temp = new File(url.getFile());
			FileInputStream stream = new FileInputStream(temp);
			props.load(stream);
			stream.close();
		} finally {
			//May set some default values.
		}
		return props;
	}
}
