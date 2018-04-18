package com.stormpractice.eventhandler.bolts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.stormpractice.eventhandler.Event;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

// For logging
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class CountIpGroupByUserBolt extends BaseBasicBolt {
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(CountIpGroupByUserBolt.class);
    //For holding words and counts
    Map<String, Set<String>> ipPerUser = new HashMap<String, Set<String>>();


    // Default constructor
    public CountIpGroupByUserBolt() {
    }


    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        //Get the word contents from the tuple
        //logger.info("Got: "+ tuple);
        String user = tuple.getString(0);
        String ipAddress = tuple.getString(1);
        Event event = (Event)tuple.getValue(2);

        Set<String> ipAddresses = ipPerUser.get(user);

        //Have we counted any already for this IP?
        if (ipAddresses == null) {
            ipAddresses = new HashSet<String>();
            ipAddresses.add(ipAddress);
            ipPerUser.put(user, ipAddresses);
        }
        else {
            ipAddresses.add(ipAddress);
        }

        ////logger.info("User: " + user + ", IP Count:" + ipAddresses.size());
        event.setCountForIp(ipAddresses.size());

        collector.emit(new Values(event.getEventId(),event));
    }

    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event-id", "event"));
    }
}