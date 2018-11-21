package trident.topo;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.testing.Split;

public class MyTridentTopo {

	public static void main(String[] args) {
		

		String zkHosts = "mpc5:2181,mpc6:2181,mpc7:2181";
		String topic = "mytri";

		TridentKafkaConfig config = new TridentKafkaConfig(
				new ZkHosts(zkHosts), topic);

		config.forceFromStart = false;
		config.scheme = new SchemeAsMultiScheme(new StringScheme());

		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(
				config);

		TridentTopology topo = new TridentTopology();

		topo.newStream("me", spout)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY), new Split(),
						new Fields("test")).groupBy(new Fields("test"));
	}
}
