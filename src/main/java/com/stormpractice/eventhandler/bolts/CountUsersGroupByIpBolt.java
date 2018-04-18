package com.stormpractice.eventhandler.bolts;

import com.stormpractice.eventhandler.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// For logging

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class CountUsersGroupByIpBolt extends BaseBasicBolt {
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(CountUsersGroupByIpBolt.class);
    //For holding words and counts
    Map<String, Set<String>> userPerIp = new HashMap<String, Set<String>>();


    // Default constructor
    public CountUsersGroupByIpBolt() {
    }


    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

            //Get the word contents from the tuple
            //logger.info("Got: "+ tuple);
            String user = tuple.getString(0);
            String ipAddress = tuple.getString(1);
            Event event = (Event)tuple.getValue(2);

            Set<String> users = userPerIp.get(ipAddress);

            //Have we counted any already for this IP?
            if (users == null) {
                users = new HashSet<String>();
                users.add(user);
                userPerIp.put(ipAddress, users);
            }
            else {
                users.add(user);
            }

            //logger.info("IP: " + ipAddress + ", User Count:" + users.size());
            event.setCountForUser(users.size());
            collector.emit(new Values(event.getEventId(),event));
    }

    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event-id", "event"));
    }
}