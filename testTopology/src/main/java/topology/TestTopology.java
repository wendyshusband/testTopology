package topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by kailin on 2/6/17.
 */
public class TestTopology {
    public static class SendDouble extends BaseRichBolt{
        public transient OutputCollector collector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] sentenceSplit = sentence.split(" ");
            for (int i=0; i<sentenceSplit.length; i++) {
                //collector.emit(tuple, new Values(sentenceSplit[i]));
                collector.emit(new Values(sentenceSplit[i]));
            }
            //collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class Receive extends BaseRichBolt{
        public transient OutputCollector collector;
        public int taskid;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
            taskid  = topologyContext.getThisTaskId();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");

            //collector.emit(tuple,new Values(word+"!"));
            //System.out.println(taskid+"output result:"+word+":"+count);
            collector.emit(new Values(taskid,word+"!"));
            //collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("task","receive"));
        }
    }


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new Spout(), 1);
        builder.setBolt("senddouble", new SendDouble(), 1).shuffleGrouping("spout");
        builder.setBolt("receive", new Receive(), 1).shuffleGrouping("senddouble");
        builder.setBolt("out", new OutBolt(),1).shuffleGrouping("receive");
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        //StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

        try {
            Thread.sleep(100000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.shutdown();
    }
}
