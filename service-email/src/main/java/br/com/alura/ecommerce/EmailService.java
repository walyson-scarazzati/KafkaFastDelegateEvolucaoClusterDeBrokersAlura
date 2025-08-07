package br.com.alura.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		EmailService emailService = new EmailService();
		try (KafkaService service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL",
				emailService::parse, Email.class, Map.of())) {
			service.run();
		}
	}

	public void parse(ConsumerRecord<String, Email> record) {
		System.out.println("-----------------");
		System.out.println("Sending email");
		System.out.println("key: " + record.key());
		System.out.println("value: " + record.value());
		System.out.println("partition: " + record.partition());
		System.out.println("Offset: " + record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Email sent successfully");
	}

}
