package topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kailin on 2/6/17.
 */
public class OutBolt extends BaseRichBolt{

    public transient OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector =outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String t =tuple.getStringByField("receive");
        t =t+"s";
        //collector.emit(tuple,new Values(tuple.getString(0)+"final"));
        collector.emit(new Values(t+"final"));
        //collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("final"));
    }
}
