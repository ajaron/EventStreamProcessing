package com.stormpractice.eventhandler.bolts;

import com.stormpractice.eventhandler.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

// For logging

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class VerdictBolt extends BaseBasicBolt {
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(VerdictBolt.class);
    //For holding words and counts
    Map<UUID, Event> events = new HashMap<UUID, Event>();

    int treshold = 20;

    // Default constructor
    public VerdictBolt(int treshold) {
        this.treshold = treshold;
    }


    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {


            //Get the word contents from the tuple
            //logger.info("Got: "+ tuple);
            UUID eventIdFromStream = (UUID)tuple.getValue(0);
            Event eventFromStream = (Event)tuple.getValue(1);
            Event eventInCache;

            if (events.containsKey(eventIdFromStream)) {
                eventInCache = events.get(eventIdFromStream);
                if ("count-ip-cross-user".equals(tuple.getSourceComponent())) {
                    // this came from IP check stream and we already has the User count data
                    eventInCache.setCountForIp(eventFromStream.getCountForIp());

                } else if ("count-user-different-ip".equals(tuple.getSourceComponent())) {
                    // this came from user check stream
                    eventInCache.setCountForUser(eventFromStream.getCountForUser());
                }

                if(isBad(eventInCache.getCountForUser(), eventInCache.getCountForIp())) {
                    genAlert(eventInCache);
                }

                //clear event from cache after we got both IP and User counts
                events.remove(eventInCache.getEventId());
            }
            else {
                // this is the first time event came so we just set it and waiting for the other bolt to come
                events.put(eventIdFromStream,eventFromStream);
            }







    }

    private boolean isBad(int countForUser, int countForIp) {

        return (countForUser + countForIp > treshold);

    }

    private void genAlert(Event event) {
        System.out.println("\nALERT - LIMIT WAS EXCEEDED - limit is: " + treshold + "\n"
                + "Event ID: " + event.getEventId() + "\n"
                + "User ID: " + event.getUserId() + "\n"
                + "IP: " + event.getIpAddress() + "\n"
                + "Count of IP cross users: " + event.getCountForIp() + "\n"
                + "Count of users per IP: " + event.getCountForUser() + "\n");
    }

    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}