package trident.topo;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import trident.teststate.TestHbaseAggregateState;
import trident.testtool.TestSplitAmt;
import trident.testtool.TestUpper;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

public class TestTirdentTopo {

	public static StormTopology getTopo(LocalDRPC drpc) {

		StateFactory state = TestHbaseAggregateState.transactional("state");

		// kafka生成的数据格式如下
		// 订单id 订单金额 创建时间 省份id
		// order_id order_amt create_time province_id
		String topic = "mytri";
		ZkHosts hosts = new ZkHosts("mpc5:2181,mpc6:2181,mpc7:2181");

		TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic);
		config.forceFromStart = false;
		config.scheme = new SchemeAsMultiScheme(new StringScheme());

		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(
				config);

		TridentTopology topo = new TridentTopology();

		topo.newStream("spout", spout)
				.parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY),
						new TestSplitAmt("\\t"),
						new Fields("order_id", "order_amt", "create_date",
								"province_id"))
				.shuffle()
				.partitionPersist(state,
						new Fields("province_id", "order_amt"), new TestUpper())
		// .groupBy(new Fields("create_date", "province_id"))
		// .persistentAggregate(state, new Fields("order_amt"), new Sum(),
		// new Fields("sum_amt"))
		;

		return topo.build();
	}

}
