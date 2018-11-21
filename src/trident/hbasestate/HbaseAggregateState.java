package trident.hbasestate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;

import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

@SuppressWarnings("rawtypes")
public class HbaseAggregateState<T> implements IBackingMap<T> {

	private HbaseConnector connector;
	private Serializer<T> serializer;

	@SuppressWarnings("unchecked")
	public HbaseAggregateState(TridentConfig<T> config) {
		this.serializer = config.getStateSerializer();
		try {
			this.connector = new HbaseConnector(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static StateFactory opaque(TridentConfig<OpaqueValue> config) {
		return new HBaseAggregateFactory(config, StateType.OPAQUE);
	}

	public static StateFactory transactional(
			TridentConfig<TransactionalValue> config) {
		return new HBaseAggregateFactory(config, StateType.TRANSACTIONAL);
	}

	public static StateFactory nonTransactional(
			TridentConfig<OpaqueValue> config) {
		return new HBaseAggregateFactory(config, StateType.NON_TRANSACTIONAL);
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<Get> gets = new ArrayList<Get>(keys.size());
		byte[] rk;
		byte[] cf;
		byte[] cq;
		for (List<Object> k : keys) {
			rk = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(0));
			cf = org.apache.hadoop.hbase.util.Bytes.toBytes("cf");
			cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(1));
			Get get = new Get(rk);
			gets.add(get.addColumn(cf, cq));
		}

		org.apache.hadoop.hbase.client.Result[] results = null;
		try {
			results = connector.getTable().get(gets);
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<T> rtn = new ArrayList<T>(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			cf = org.apache.hadoop.hbase.util.Bytes.toBytes("cf");
			cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String) keys
					.get(i).get(1));
			org.apache.hadoop.hbase.client.Result result = results[i];
			if (result.isEmpty()) {
				rtn.add(null);
			} else {
				rtn.add((T) serializer.deserialize(result.getValue(cf, cq)));
			}

		}
		return rtn;
	}

	@Override
	public void multiPut(List<List<Object>> k, List<T> vals) {
		List<Put> puts = new ArrayList<Put>();

		for (int i = 0; i < k.size(); i++) {
			byte[] rk = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k
					.get(i).get(0));
			byte[] cf = org.apache.hadoop.hbase.util.Bytes.toBytes("cf");
			byte[] cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k
					.get(i).get(1));

			byte[] cv = serializer.serialize(vals.get(i));
			Put p = new Put(rk);
			puts.add(p.add(cf, cq, cv));
		}

		try {
			connector.getTable().put(puts);
			connector.getTable().flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
