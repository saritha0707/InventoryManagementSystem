package com.oms.service;

import com.oms.entity.Orders;
import com.oms.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

public class KafkaConsumerService {
    @Autowired
    private OrderRepository orderRepository;

    @KafkaListener(topics = "order-events", groupId = "order-group")
    public void consume(Orders order) {

        System.out.println("Received order: " + order.getOrderId());

        // Simulate payment processing
        order.setStatus("PAID");

        orderRepository.save(order);
    }
}
