package trident.testtool;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TestSplitAmt extends BaseFunction {

	private static final long serialVersionUID = -5570560362665224198L;
	private String septor;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		String line = tuple.getString(0);
		String[] fields = StringUtils.split(line, this.septor);
		// order_id order_amt create_time province_id
		collector.emit(new Values(fields[0], Double.parseDouble(fields[1]),
				fields[2], "amt_" + fields[3]));

	}

	public TestSplitAmt(String septor) {
		super();
		this.septor = septor;
	}

	/**
	 * @return the septor
	 */
	public String getSeptor() {
		return septor;
	}

	/**
	 * @param septor
	 *            the septor to set
	 */
	public void setSeptor(String septor) {
		this.septor = septor;
	}

}
