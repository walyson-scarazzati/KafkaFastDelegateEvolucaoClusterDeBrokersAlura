package br.com.alura.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var fraudDetectorService = new FraudDetectorService();
		try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudDetectorService::parse, Order.class, Map.of())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("-----------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println("key: " + record.key());
		System.out.println("value: " + record.value());
		System.out.println("partition: " + record.partition());
		System.out.println("Offset: " + record.offset());

		try {
			Thread.sleep(5000); // Simulating processing time
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
