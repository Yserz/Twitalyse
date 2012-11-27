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
	 * @param url
	 *            a URL
	 * @return the source
	 */
	public static String findSource(String url) {
		if (url.equals("web")) {
			return url;
		}else{
			Pattern p = Pattern.compile("<.*>(.*)<.*>");

			Matcher m = p.matcher(url);
			return (m.matches()) ? m.group(1) : url;
		}
	}
}
