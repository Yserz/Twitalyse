/*
 * Copyright (C) 2012 Michael Koppen
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
package de.fhb.twitalyse.bolt.data;

import java.util.List;
/**
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class Status{
	public List<Long> contributors;
	public Coordinate coordinates;
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
