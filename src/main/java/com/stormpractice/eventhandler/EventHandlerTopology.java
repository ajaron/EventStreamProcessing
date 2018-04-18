package com.stormpractice.eventhandler;

import com.stormpractice.eventhandler.bolts.CountIpGroupByUserBolt;
import com.stormpractice.eventhandler.bolts.CountUsersGroupByIpBolt;
import com.stormpractice.eventhandler.bolts.InitialParserBolt;
import com.stormpractice.eventhandler.bolts.VerdictBolt;
import com.stormpractice.eventhandler.spouts.EventGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

public class EventHandlerTopology {

    private final static int DEFAULT_TREHOLD = 20;

    //Entry point for the topology
    public static void main(String[] args) throws Exception {
        int treshold = DEFAULT_TREHOLD;
        if(args.length > 0 && args[0] != null) {
            try {
                treshold = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("INPUT ERROR - First arg is the treshold, must be int");
                return;
            }
        }

        //Used to build the topology
        TopologyBuilder builder = new TopologyBuilder();
        //Add the spout, with a name of 'spout'
        builder.setSpout("spout", new EventGeneratorSpout(), 3);
        //Add the InitialParserBolt bolt, with a name of 'parser'
        //and parallelism hint of 5 executors
        //shufflegrouping subscribes to the spout, and equally distributes
        builder.setBolt("parser", new InitialParserBolt(), 5).shuffleGrouping("spout");

        //Add the IP counter
        //and parallelism hint of 8 executors
        //fieldsgrouping subscribes to the parser bolt, and
        //ensures that the same user is sent to the same instance (group by field 'user')
        builder.setBolt("count-ip-cross-user", new CountIpGroupByUserBolt(), 8).fieldsGrouping("parser", new Fields("user"));

        //Add the User counter
        //and parallelism hint of 8 executors
        //fieldsgrouping subscribes to the parser bolt, and
        //ensures that the same IP is sent to the same instance (group by field 'ipAddress')
        builder.setBolt("count-user-different-ip", new CountUsersGroupByIpBolt(), 8).fieldsGrouping("parser", new Fields("ipAddress"));


        //Add the Verdict bolt which check the treshold of both counters
        //and parallelism hint of 12 executors
        //double fieldsgrouping subscribes to the count-ip-cross-user and count-user-different-ip bolts, and
        //ensures that the same event is sent to the same instance (group by field 'event-id')
        BoltDeclarer bd =  builder.setBolt("verdict", new VerdictBolt(treshold), 12);
        bd.fieldsGrouping("count-ip-cross-user",new Fields("event-id"));
        bd.fieldsGrouping("count-user-different-ip",new Fields("event-id"));

        //new configuration
        Config conf = new Config();
        //Set to false to disable debug information when
        // running in production on a cluster
        conf.setDebug(false);

        //Otherwise, we are running locally

        //Cap the maximum number of executors that can be spawned
        //for a component to 3
        conf.setMaxTaskParallelism(3);
        //LocalCluster is used to run locally
        LocalCluster cluster = new LocalCluster();
        //submit the topology
        cluster.submitTopology("event-handler", conf, builder.createTopology());

        Thread.sleep(120000);
        //shut down the cluster
        cluster.shutdown();

    }
}