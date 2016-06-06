package org.junkie.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junkie.storm.bolts.WordCounter;
import org.junkie.storm.bolts.WordNormalizer;
import org.junkie.storm.spouts.WordReader;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		/*
		 * Topology definition
		 */
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
				.shuffleGrouping("word-reader");
//		builder.setBolt("word-counter", new WordCounter(),3).shuffleGrouping("word-normalizer");
		builder.setBolt("word-counter", new WordCounter(),3).fieldsGrouping(("word-normalizer"), new Fields("word"));
		
		/*
		 * Configuration
		 */
		Config conf = new Config();
		conf.put("wordsFile", "src/main/resources/word.txt");
		conf.setDebug(true);
		
		
		/*
		 * Topology run local cluster
		 */
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
		
		//remote cluster
//		StormSubmitter.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
	}

}
