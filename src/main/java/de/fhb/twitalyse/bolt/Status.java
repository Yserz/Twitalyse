
package de.fhb.twitalyse.bolt;

import java.util.List;

public class Status{
	public List<Long> contributors;
	public List<Float> coordinates;
   	public String created_at;
   	public Entity entities;
   	public Boolean favorited;
   	public String id_str;
   	public String in_reply_to_screen_name;
   	public String in_reply_to_status_id_str;
   	public String in_reply_to_user_id_str;
   	public Place place;
   	public Long retweet_count;
   	public Boolean retweeted;
   	public String source;
   	public String text;
   	public Boolean truncated;
   	public User user;

}
