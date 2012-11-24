/*
 * Copyright (C) 2012 Andy Klay
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
package de.fhb.twitalyse.bolt.statustext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import de.fhb.twitalyse.bolt.Status;
import java.util.Map;

/**
 * This Bolt gets the Twitter Retweet Count out of the whole Status.
 *
 * @author Andy Klay <klay@fh-brandenburg.de>
 */
public class SplitRetweetCounterBolt extends BaseRichBolt{
    
    
       private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
		long id = input.getLong(0);
		System.out.println("SplitRetweetCountBolt Status ID: " + id);
		String json = input.getString(1);

		try {
			Gson gson = new Gson();
			Status ts = gson.fromJson(json, Status.class);
			System.out.println("SplitRetweetCountBolt Retweet Status : " + ts.retweet_count);
                        
			collector.emit(input, new Values(id, ts.retweet_count));
			collector.ack(input);
		} catch (RuntimeException re) {
			System.out.println("########################################################");
			System.out.println("Exception: "+re);
			System.out.println("JSON: "+json);
			System.out.println("########################################################");
		}
    }

}