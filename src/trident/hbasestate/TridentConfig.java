package trident.hbasestate;

import java.util.HashMap;
import java.util.Map;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.StateType;

@SuppressWarnings("rawtypes")
public class TridentConfig<T> extends TupleTableConfig {

	private static final long serialVersionUID = 7011250469206178117L;

	private int stateCacheSize = 1000;
	private Serializer stateSerializer;

	public TridentConfig(String table) {
		super(table);
	}

	public TridentConfig(String table, String rowkeyField,
			String tupleTimestampField) {
		super(table, rowkeyField, tupleTimestampField);
	}

	public int getStateCacheSize() {
		return stateCacheSize;
	}

	public void setStateCacheSize(int stateCacheSize) {
		this.stateCacheSize = stateCacheSize;
	}

	public static final Map<StateType, Serializer> DEFAULT_SERIALIZES = new HashMap<StateType, Serializer>() {

		private static final long serialVersionUID = 2703742357391463162L;

		{
			put(StateType.NON_TRANSACTIONAL,
					new JSONNonTransactionalSerializer());
			put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
			put(StateType.OPAQUE, new JSONOpaqueSerializer());
		}

	};

	public Serializer getStateSerializer() {
		return stateSerializer;
	}

	public void setStateSerializer(Serializer stateSerializer) {
		this.stateSerializer = stateSerializer;
	}

}
