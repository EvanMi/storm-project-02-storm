package trident.msgproducer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.utils.Utils;

public class KafkaMessageProducer implements Runnable {
	private final Producer<String, String> producer;
	private final String topic;

	public KafkaMessageProducer(String topic) {
		Properties props = new Properties();
		props.put("zk.connect", "mpc5:2181,mpc6:2181,mpc7:2181");
		props.put("metadata.broker.list", "mpc5:9092,mpc6:9092,mpc7:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		this.topic = topic;
	}

	@Override
	public void run() {
		// order_id order_amt create_time province_id
		Random random = new Random();
		String[] order_amt = { "10.10", "20.10", "5.2", "6.0", "8.1" };
		String[] area_id = { "1", "2", "3", "4", "5", "6", "7", "8", "9" };
		int i = 0;
		while (true) {
			i++;
			String messageStr = i + "\t" + order_amt[random.nextInt(5)] + "\t"
					+ new SimpleDateFormat("yyyy-MM-dd").format(new Date())
					+ "\t" + area_id[random.nextInt(9)];
			System.out.println("product:" + messageStr);
			producer.send(new KeyedMessage<String, String>(topic, "part0",
					messageStr));
			Utils.sleep(100);

			

		}

	}

	public static void main(String[] args) {
		Thread thread = new Thread(new KafkaMessageProducer("mytri3"));
		thread.start();
	}
}
