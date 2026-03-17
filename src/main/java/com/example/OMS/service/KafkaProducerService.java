package com.example.OMS.service;

import com.example.OMS.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    private static final String TOPIC = "order-events";

    public void sendOrderEvent(Order order) {
        kafkaTemplate.send(TOPIC, order);
        System.out.println("Order event sent: " + order.getId());
    }
}
