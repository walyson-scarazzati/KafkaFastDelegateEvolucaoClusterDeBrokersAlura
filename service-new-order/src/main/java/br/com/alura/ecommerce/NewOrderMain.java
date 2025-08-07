package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// no KafkaProducer preciso de parametros de tipagem tipo da chave e o tipo da
		// mensagem nesse caso vamos usar string em tudo
		try (var orderDispatcher = new KafkaDispatcher<Order>()) {
			try (var emailDispatcher = new KafkaDispatcher<Email>()) {
				for (int i = 0; i < 10; i++) {
					String userId = UUID.randomUUID().toString();
					String orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					var order = new Order(userId, orderId, amount);
					String value = userId + "132123,67523,789289745";
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

					var email = new Email("Assunto do email",
							"Corpo do email: Thank you for your order! We are processing it now.");
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
				}
			}
		}
	}

}
