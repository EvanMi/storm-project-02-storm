package trident.myhbasestate;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MyHbaseAggregateStateFactory implements StateFactory {

	private static final long serialVersionUID = 6504244622155615883L;

	private StateType stateType;
	private String tableName;
	private int stateCacheSize = 1000;

	public MyHbaseAggregateStateFactory(String tableName, StateType stateType) {

		this.tableName = tableName;
		this.stateType = stateType;
	}

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {

		MyHbaseAggregateState myHbaseAggregateState = new MyHbaseAggregateState(
				tableName, stateType);
		CachedMap cachedMap = new CachedMap(myHbaseAggregateState,
				stateCacheSize);

		MapState mapState = null;
		if (stateType.equals(StateType.NON_TRANSACTIONAL)) {
			mapState = NonTransactionalMap.build(cachedMap);

		} else if (stateType.equals(StateType.TRANSACTIONAL)) {

			mapState = TransactionalMap.build(cachedMap);

		} else if (stateType.equals(StateType.OPAQUE)) {

			mapState = OpaqueMap.build(cachedMap);

		}

		return new SnapshottableMap(mapState, new Values("$GLOBAL$"));
	}

}
