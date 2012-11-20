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
package de.fhb.twitalyse;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.fhb.twitalyse.bolt.statustext.AnalyseStatusTextBolt;
import de.fhb.twitalyse.spout.TwitterStreamSpout;

/**
 * 
 * @author Michael Koppen <koppen@fh-brandenburg.de>
 */
public class TwitalyseTopology {

	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();
		
		TwitterStreamSpout spout = new TwitterStreamSpout();
		AnalyseStatusTextBolt bolt = new AnalyseStatusTextBolt();

		builder.setSpout("spout", spout, 1);
		builder.setBolt("textBolt", bolt)
				.shuffleGrouping("spout");

		Config conf = new Config();
        conf.setDebug(true);

        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitalyse", conf, builder.createTopology());
        
            Utils.sleep(10000);

            cluster.shutdown();
        }
	}
}
