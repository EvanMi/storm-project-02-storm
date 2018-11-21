package trident.tools;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class Print extends BaseFunction {

	private static final long serialVersionUID = -705869775976703437L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if (!tuple.isEmpty()) {
			String message = tuple.getString(0);
			System.out.println(message);
		}
	}

}
