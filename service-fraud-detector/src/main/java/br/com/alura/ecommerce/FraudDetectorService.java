package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
	public static void main(String[] args) {
		var fraudDetectorService = new FraudDetectorService();
		try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudDetectorService::parse, Order.class, Map.of())) {
			service.run();
		}
	}
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException  {
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
		 var order = record.value();
		 if(isFraud(order)) {
	            // pretending that the fraud happens when the amount is >= 4500
			System.out.println("Order is a fraud! " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
		 } else {
			 System.out.println("Approved order: " + order);
			 orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
		 }   
	   
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

}
