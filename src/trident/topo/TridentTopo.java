package trident.topo;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import trident.hbasestate.HbaseAggregateState;
import trident.hbasestate.TridentConfig;
import trident.tools.OrderAmtSplit;
import trident.tools.OrderNumSplit;
import trident.tools.Split;
import trident.tools.SplitBy;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

public class TridentTopo {

	public static StormTopology buildTopo(LocalDRPC drpc) {
		TridentConfig tridentConfig = new TridentConfig("state");
		StateFactory state = HbaseAggregateState.transactional(tridentConfig);

		// StateFactory state = new MyHbaseAggregateState("state",
		// StateType.TRANSACTIONAL).getFactory();

		ZkHosts zkHosts = new ZkHosts("mpc5:2181,mpc6:2181,mpc7:2181");
		String topStr = "mytri3";
		TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topStr);

		config.forceFromStart = false;
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		// config.fetchSizeBytes=10;

		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(
				config);

		TridentTopology topo = new TridentTopology();

		// 销售额
		TridentState amtState = topo
				.newStream("spout", spout)
				.parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY),
						new OrderAmtSplit("\t"),
						new Fields("order_id", "order_amt", "create_date",
								"province_id"))
				.shuffle()
				.groupBy(new Fields("create_date", "province_id"))
				.persistentAggregate(state, new Fields("order_amt"), new Sum(),
						new Fields("sum_amt"));

		topo.newDRPCStream("getOrderAmt", drpc)
				.each(new Fields("args"), new Split(" "), new Fields("arg"))
				.each(new Fields("arg"), new SplitBy("\\:"),
						new Fields("create_date", "province_id"))
				.groupBy(new Fields("create_date", "province_id"))
				.stateQuery(amtState, new Fields("create_date", "province_id"),
						new MapGet(), new Fields("sum_amt"))
				.applyAssembly(new FirstN(5, "sum_amt", true));

		// 订单数
		TridentState orderState = topo
				.newStream("orderSpout", spout)
				.parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY),
						new OrderNumSplit("\t"),
						new Fields("order_id", "order_amt", "create_date",
								"province_id"))
				.shuffle()
				.groupBy(new Fields("create_date", "province_id"))
				.persistentAggregate(state, new Fields("province_id"),
						new Count(), new Fields("order_num"));

		topo.newDRPCStream("getOrderNum", drpc)
				.each(new Fields("args"), new Split(" "), new Fields("arg"))
				.each(new Fields("arg"), new SplitBy("\\:"),
						new Fields("create_date", "province_id"))
				.groupBy(new Fields("create_date", "province_id"))
				.stateQuery(orderState,
						new Fields("create_date", "province_id"), new MapGet(),
						new Fields("order_num"))
		// .applyAssembly(new FirstN(5, "order_num", true))
		;

		return topo.build();

	}

	public static void main(String[] args) {

		LocalDRPC drpc = new LocalDRPC();

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(4);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, buildTopo(null));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {

			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, buildTopo(drpc));
		}

		// while (true) {
		// System.err
		// .println(drpc
		// .execute(
		// "getOrderAmt",
		// "2016-09-30:1 2016-09-30:2 2016-09-30:3 2016-09-30:4 2016-09-30:5 2016-09-30:6 2016-09-30:7 2016-09-30:8 2016-09-30:9"));
		// System.out
		// .println(drpc
		// .execute(
		// "getOrderNum",
		// "2016-09-30:1 2016-09-30:2 2016-09-30:3 2016-09-30:4 2016-09-30:5 2016-09-30:6 2016-09-30:7 2016-09-30:8 2016-09-30:9"));
		// Utils.sleep(2000);
		// }

	}
}
