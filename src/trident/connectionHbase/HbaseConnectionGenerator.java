package trident.connectionHbase;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HbaseConnectionGenerator {
	private static HConnection connection = null;

	private HbaseConnectionGenerator() {

	}

	public static HConnection getConnection() {

		if (null == connection) {
			try {
				Configuration conf = HBaseConfiguration.create();
				String zkHost = "mpc5:2181,mpc6:2181,mpc7:2181";
				conf.set("hbase.zookeeper.quorum", zkHost);
				ExecutorService pool = Executors.newFixedThreadPool(10);
				connection = HConnectionManager.createConnection(conf, pool);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		return connection;

	}

}
