package de.fhb.twitalyse.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterUtils {

	/**
	 * Finds the source of a url.
	 * <table border=1>
	 * <tbody>
	 * <tr>
	 * <td><b>Input<b></td>
	 * <td><b>Output<b></td>
	 * </tr>
	 * <tr>
	 * <td>http://twitter.com/download/android</td>
	 * <td>android</td>
	 * </tr>
	 * <tr>
	 * <td>http://twitter.com/download/iphone</td>
	 * <td>iphone</td>
	 * </tr>
	 * <tr>
	 * <td>http://blackberry.com/twittertd>
	 * <td>blackberry</td>
	 * </tr>
	 * <tr>
	 * <td>https://foo.com/bar</td>
	 * <td>foo.com</td>
	 * </tr>
	 * <tr>
	 * <td>http://www.foo.com#bar</td>
	 * <td>foo.com</td>
	 * </tr>
	 * <tr>
	 * <td>http://bar.foo.com</td>
	 * <td>foo.com</td>
	 * </tr>
	 * <tr>
	 * <td>https://mail.google.com/mail/ca</td>
	 * <td>google.com</td>
	 * </tr>
	 * </tbody>
	 * </table>
	 * 
	 * @param url a URL
	 * @return the source
	 */
	public static String findSource(String url) {
		Pattern p = Pattern.compile(".*?([^.]+\\.[^.]+)");
		try {
			URI uri = new URI(url);
			String path = uri.getPath();
			if (path.contains("twitter.com/download/android")) {
				return "android";
			}
			if (path.contains("twitter.com/download/iphone")) {
				return "iphone";
			}
			if (path.contains("blackberry.com/twitter")) {
				return "blackberry";
			}
			if (uri.getHost() != null) {
				Matcher m = p.matcher(uri.getHost());
				return (m.matches()) ? m.group(1) : uri.toString();
			} else {
				return uri.toString();
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return url;
		}
	}
}
