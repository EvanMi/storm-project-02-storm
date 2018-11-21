package trident.myhbasestate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.IBackingMap;

@SuppressWarnings("rawtypes")
public class MyHbaseAggregateState<T> implements IBackingMap<T> {

	HConnection hTablePool = null;
	String tableName = null;
	StateType stateType = null;

	Map<StateType, Serializer> serializers = new HashMap<StateType, Serializer>();

	public MyHbaseAggregateState(String tableName, StateType stateType) {
		this.tableName = tableName;
		this.stateType = stateType;
		initSerializers();
		initHConnection();
	}

	public void initSerializers() {
		serializers.put(StateType.NON_TRANSACTIONAL,
				new JSONNonTransactionalSerializer());
		serializers.put(StateType.TRANSACTIONAL,
				new JSONTransactionalSerializer());
		serializers.put(StateType.OPAQUE, new JSONOpaqueSerializer());
		
		
		new JSONTransactionalSerializer();

	}

	@SuppressWarnings("unchecked")
	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		ArrayList<Get> gets = new ArrayList<Get>(keys.size());

		byte[] rowkey;
		byte[] cf = Bytes.toBytes("cf");
		byte[] cq;

		for (List<Object> key : keys) {

			rowkey = Bytes.toBytes((String) key.get(0));
			cq = Bytes.toBytes((String) key.get(1));

			Get get = new Get(rowkey);
			get.addColumn(cf, cq);
			gets.add(get);

		}
		ArrayList<T> rtn = new ArrayList<T>(keys.size());
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Result[] results = table.get(gets);

			for (int i = 0; i < keys.size(); i++) {
				Result result = results[i];

				cq = Bytes.toBytes((String) keys.get(i).get(1));
				if (result.isEmpty()) {
					rtn.add(null);
				} else {
					rtn.add((T) serializers.get(stateType).deserialize(
							result.getValue(cf, cq)));
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return rtn;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {

		ArrayList<Put> puts = new ArrayList<Put>(keys.size());

		byte[] rowkey;
		byte[] cq;
		byte[] cv;

		for (int i = 0; i < keys.size(); i++) {
			rowkey = Bytes.toBytes((String) keys.get(i).get(0));
			cq = Bytes.toBytes((String) keys.get(i).get(1));

			cv = serializers.get(stateType).serialize(vals.get(i));

			Put put = new Put(rowkey);
			put.add(Bytes.toBytes("cf"), cq, cv);

			puts.add(put);
		}

		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(puts);
			table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void initHConnection() {

		Configuration conf = new Configuration();
		String zkHost = "mpc5:2181,mpc6:2181,mpc7:2181";
		conf.set("hbase.zookeeper.quorum", zkHost);

		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public StateFactory getFactory() {
		return new MyHbaseAggregateStateFactory(tableName, stateType);
	}

}
