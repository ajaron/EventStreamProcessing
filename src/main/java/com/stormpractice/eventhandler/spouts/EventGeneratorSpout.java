package com.stormpractice.eventhandler.spouts;

import com.stormpractice.eventhandler.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

//This spout randomly emits sentences
public class EventGeneratorSpout extends BaseRichSpout {

    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(EventGeneratorSpout.class);

    //Collector used to emit output
    SpoutOutputCollector _collector;


    //Open is called when an instance of the class is created
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //Set the instance collector to the one passed in
        _collector = collector;
    }

    //Emit data to the stream
    @Override
    public void nextTuple() {
        //Sleep for a bit
        Utils.sleep(100);

        ////logger.info("Event generated:" + event);

        //Emit the sentence
        _collector.emit(new Values(Event.genEvent()));
    }

    //Ack is not implemented since this is a basic example
    @Override
    public void ack(Object id) {
    }

    //Fail is not implemented since this is a basic example
    @Override
    public void fail(Object id) {
    }

    //Declare the output fields. In this case, an sentence
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }

}