package de.fhb.twitalyse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

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
			ClassLoader loader = ClassLoader.getSystemClassLoader();
			InputStream in = loader.getResourceAsStream(path);
			props.load(in);
			
			
		} finally {
			//May set some default values.
		}
		return props;
	}
}
