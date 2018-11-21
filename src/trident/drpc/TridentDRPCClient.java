package trident.drpc;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;

public class TridentDRPCClient {

	public static void main(String[] args) throws TException,
			DRPCExecutionException {

		DRPCClient client = new DRPCClient("mpc1", 3772);

		while (true) {
			System.err
					.println(client
							.execute(
									"getOrderAmt",
									"2016-10-08:amt_1 2016-10-08:amt_2 2016-10-08:amt_3 2016-10-08:amt_4 2016-10-08:amt_5"));
			System.out
					.println(client
							.execute(
									"getOrderNum",
									"2016-10-08:num_1 2016-10-08:num_2 2016-10-08:num_3 2016-10-08:num_4 2016-10-08:num_5"));
			Utils.sleep(5000);
		}
	}

}
