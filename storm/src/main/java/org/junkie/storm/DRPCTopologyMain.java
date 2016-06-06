package org.junkie.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.junkie.storm.bolts.AdderBolt;

public class DRPCTopologyMain {

	public static void main(String[] args) {
		// Create the local drpc client/server
		LocalDRPC drpc = new LocalDRPC();

		// Create the drpc topology and specify the function name.
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
		builder.addBolt(new AdderBolt());

		Config conf = new Config();
		conf.setDebug(true);

		// Create cluster and submit the topology
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("drpc-adder-topology", conf,
				builder.createLocalTopology(drpc));
		
		//Test the topology
		String result = drpc.execute("add", "1+2+3+4+5");
		System.out.println("####### result ---> "+result+" #######");
		
		cluster.shutdown();
		drpc.shutdown();
	}

}
