package trident.teststate;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HbaseConnectionGenerater {

	private static HConnection connection = null;

	private HbaseConnectionGenerater() {
	}

	public static final HConnection generateHbaseConnection() {
		Configuration conf = HBaseConfiguration.create();
		String zkHosts = "mpc5:2181,mpc6:2181,mpc7:2181";
		conf.set("hbase.zookeeper.quorum", zkHosts);
		ExecutorService pool = Executors.newFixedThreadPool(10);
		if (null == connection) {
			try {
				connection = HConnectionManager.createConnection(conf, pool);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return connection;

	}

}
