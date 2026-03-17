package com.example.OMS.service;

import com.example.OMS.entity.Order;
import com.example.OMS.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

public class KafkaConsumerService {
    @Autowired
    private OrderRepository orderRepository;

    @KafkaListener(topics = "order-events", groupId = "order-group")
    public void consume(Order order) {

        System.out.println("Received order: " + order.getId());

        // Simulate payment processing
        order.setStatus("PAID");

        orderRepository.save(order);
    }
}
