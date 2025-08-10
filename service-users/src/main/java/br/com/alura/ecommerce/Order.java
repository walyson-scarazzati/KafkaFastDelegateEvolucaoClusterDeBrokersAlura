package br.com.alura.ecommerce;

import java.math.BigDecimal;

public class Order {
	private final String userId, orderId;
	private final BigDecimal amount;

	Order(String userId, String orderId, BigDecimal amount) {
		this.userId = userId;
		this.orderId = orderId;
		this.amount = amount;
	}

	public String getEmail() {
		// TODO Auto-generated method stub
		return "email";
	}

}
