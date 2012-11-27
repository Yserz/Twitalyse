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
		assertEquals("cpan.org", TwitterUtils.findSource("<a href=\"http://search.cpan.org/~rosch/URI-Find-0.16/lib/URI/Find.pm\">cpan.org</a>"));
		assertEquals("web", TwitterUtils.findSource("web"));
		assertEquals("Twitter for iPhone", TwitterUtils.findSource("<a href=\"http://twitter.com/download/iPhone\">Twitter for iPhone</a>"));
		assertEquals("Twitter for Android", TwitterUtils.findSource("<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>"));
		assertEquals("TweetList!", TwitterUtils.findSource("<a href=\"http://tweetli.st/\" rel=\"nofollow\">TweetList!</a>"));
	}
}
