package trident.testtool;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.TransactionalMap;
import storm.trident.tuple.TridentTuple;

public class TestUpper<T> extends BaseStateUpdater<TransactionalMap<T>> {

	private static final long serialVersionUID = 7874631076851480556L;

	@SuppressWarnings("unchecked")
	@Override
	public void updateState(TransactionalMap<T> state,
			List<TridentTuple> tuples, TridentCollector collector) {

		List<List<Object>> keys = new ArrayList<List<Object>>();
		List<T> values = new ArrayList<T>();
		for (TridentTuple tuple : tuples) {
			ArrayList<Object> tem = new ArrayList<Object>();
			tem.add(tuple.getValue(0));
			keys.add(tem);
			values.add((T) tuple.getValue(1));
		}

		state.multiPut(keys, values);
	}
}
