import java.util.Properties;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerThread implements Runnable {

	private final KafkaProducer<String, String> producer;
	private final String topic;
	private final String filename;
	private final File directory;

	public ProducerThread(String brokers, String topic, String filename, File directory) {
		this.filename = filename;
		this.directory = directory;
		Properties prop = createProducerConfig(brokers);
		this.producer = new KafkaProducer<String, String>(prop);
		this.topic = topic;
	}

	private static Properties createProducerConfig(String brokers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	@Override
	public void run() {
		File tempFile = new File(this.directory, filename);
		StringBuffer strbuf = new StringBuffer();
		InputStream in = null;
		try {
			in = new java.io.FileInputStream(tempFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			in = new java.io.BufferedInputStream(in);
			try {
				while (in.available() > 0) {
					strbuf.append((char) in.read());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} finally {
			IOUtils.closeQuietly(in);
		}

		producer.send(new ProducerRecord<String, String>(topic, strbuf.toString()), new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					e.printStackTrace();
				}
				System.out.println(
						"Sent:" + strbuf + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
			}
		});

		// closes producer
		producer.close();
	}
}
