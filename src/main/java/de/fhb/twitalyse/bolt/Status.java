
package de.fhb.twitalyse.bolt;

import java.util.List;

public class Status{
//   	public List<Contributors> contributors;
	public List<Long> contributors;
//   	public Coordinates coordinates;
	public List<Float> coordinates;
   	public String created_at;
   	public Entities entities;
   	public boolean favorited;
   	public String id_str;
   	public String in_reply_to_screen_name;
   	public String in_reply_to_status_id_str;
   	public String in_reply_to_user_id_str;
   	public Place place;
   	public int retweet_count;
   	public boolean retweeted;
   	public String source;
   	public String text;
   	public boolean truncated;
   	public User user;

}
