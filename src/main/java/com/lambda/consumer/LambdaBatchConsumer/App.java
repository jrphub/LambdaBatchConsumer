package com.lambda.consumer.LambdaBatchConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Lambda Batch Consumer Application
 *
 */
public class App {
	//default values
	private static String brokers = "localhost:9092";
	private static String groupId = "tweets-batch";
	private static String topic = "tweets";
	private static String outputDir="tweetsBatchOutput";

	private static KafkaConsumer<String, String> createConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(props);
	}

	static void runConsumer() throws Exception {
		KafkaConsumer<String, String> consumer = createConsumer();
		consumer.subscribe(Collections.singletonList(topic));
		final int minBatchSize = 5;
		List<String> buffer = new ArrayList<>();
		try {
			while (true) {
				final ConsumerRecords<String, String> consumerRecords = consumer
						.poll(1000);
				consumerRecords.forEach(record -> {
					buffer.add(record.value());
					System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
							record.key(), record.value(), record.partition(),
							record.offset());
				});

				if (buffer.size() >= minBatchSize) {
					store(buffer);
					consumer.commitAsync();
					buffer.clear();
					break;
				}
			}
		} finally {
			consumer.close();
		}
	}

	private static void store(List<String> buffer) {
		long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName("LambdaBatchStore")
				.set("spark.executor.instances", "2");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> batchRDD = jsc.parallelize(buffer);

		batchRDD.saveAsTextFile("file://"
				+ outputDir + "/Tweetbkp_"
				+ startTime);
		jsc.close();
	}

	public static void main(String... args) throws Exception {
		if (args.length == 4) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
			outputDir = args[3];
		}
		runConsumer();
	}
}