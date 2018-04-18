package com.stormpractice.eventhandler.bolts;

import com.stormpractice.eventhandler.Event;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class InitialParserBolt extends BaseBasicBolt {

    //Execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //Get the sentence content from the tuple
         Event event = tuple.getValues().get(0) instanceof Event ? ((Event) tuple.getValues().get(0)) : null;;
        //An iterator to get each word

       if(event != null) {
           collector.emit(new Values(event.getUserId(), event.getIpAddress(),event));
       }
    }

    //Declare that emitted tuples contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user","ipAddress","event"));
    }
}