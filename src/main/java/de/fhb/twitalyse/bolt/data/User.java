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
/**
 *
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class User{
	//TODO: tighten datatypes Long -> Integer etc. if possible
   	public Boolean contributors_enabled;
   	public String created_at;
   	public Boolean default_profile;
   	public Boolean default_profile_image;
   	public String description;
   	public Long favourites_count;
   	public Boolean follow_request_sent;
   	public Long followers_count;
   	public Boolean following;
   	public Long friends_count;
   	public Boolean geo_enabled;
   	public Long id;
   	public String id_str;
   	public Boolean is_translator;
   	public String lang;
   	public Long listed_count;
   	public String location;
   	public String name;
   	public Boolean notifications;
   	public String profile_background_color;
   	public String profile_background_image_url;
   	public String profile_background_image_url_https;
   	public Boolean profile_background_tile;
   	public String profile_image_url;
   	public String profile_image_url_https;
   	public String profile_link_color;
   	public String profile_sidebar_border_color;
   	public String profile_sidebar_fill_color;
   	public String profile_text_color;
   	public Boolean profile_use_background_image;
//   	public Boolean protected;
   	public String screen_name;
   	public Long statuses_count;
   	public String time_zone;
   	public String url;
   	public Long utc_offset;
   	public Boolean verified;

}
