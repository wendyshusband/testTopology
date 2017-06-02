package topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by kailin on 2/6/17.
 */
public class Spout extends BaseRichSpout{

    private transient SpoutOutputCollector _collector;
    //private FrequencyRestrictor frequencyRestrictor;
    //private AtomicInteger co = new AtomicInteger(0);
    //private int number;
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
      //  frequencyRestrictor = new FrequencyRestrictor(10,10);
       // number = 2000;
    }

    @Override
    public void nextTuple() {
        //if (frequencyRestrictor.tryPermission() && co.get() < number) {
          //  co.getAndIncrement();
        Utils.sleep(100);
            String sentence = "over moon";
        _collector.emit(new Values(sentence));

            //_collector.emit(new Values(sentence), UUID.randomUUID().toString());
        //}
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}
