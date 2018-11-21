package trident.hbasestate;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HbaseConnector implements Serializable {

	private static final long serialVersionUID = 7564517733056475156L;

	private Configuration configuration;
	protected HTableInterface table;
	private String tableName;

	HConnection hTablePool = null;

	public HbaseConnector(TupleTableConfig conf) throws Exception {

		configuration = new Configuration();
		String zkHost = "mpc5:2181,mpc6:2181,mpc7:2181";
		configuration.set("hbase.zookeeper.quorum", zkHost);

		hTablePool = HConnectionManager.createConnection(configuration);

		this.tableName = conf.getTableName();
		this.table = hTablePool.getTable(this.tableName);
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public HTableInterface getTable() {
		return table;
	}

	public void setTable(HTableInterface table) {
		this.table = table;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
