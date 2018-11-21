package trident.tools;

import org.apache.commons.lang.StringUtils;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitBy extends BaseFunction {

	private static final long serialVersionUID = -1301925768704884634L;
	String partten = null;

	public SplitBy(String partten) {

		this.partten = partten;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		if (!tuple.isEmpty()) {
			String msg = tuple.getString(0);
			String[] values = StringUtils.split(msg, this.partten);

			collector.emit(new Values(values[0], values[1]));

		}
	}

}
