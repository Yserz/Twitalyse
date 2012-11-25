package de.fhb.twitalyse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import de.fhb.twitalyse.utils.TwitterUtils;

public class TwitterUtilsTest {

	@Test
	public void findSourceTest() {
		assertEquals("cpan.org", TwitterUtils.findSource("http://search.cpan.org/~rosch/URI-Find-0.16/lib/URI/Find.pm"));
		assertEquals("foo.com", TwitterUtils.findSource("https://foo.com/bar"));
		assertEquals("google.com", TwitterUtils.findSource("https://mail.google.com/mail/ca"));
		assertEquals("google.com", TwitterUtils.findSource("https://mail.google.com/mail/ca"));
		assertEquals("web", TwitterUtils.findSource("web"));
		assertEquals("android", TwitterUtils.findSource("twitter.com/download/android"));
		assertEquals("iphone", TwitterUtils.findSource("twitter.com/download/iphone"));
	}
}
