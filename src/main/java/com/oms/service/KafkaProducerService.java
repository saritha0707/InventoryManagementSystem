package com.oms.service;

import com.oms.entity.Order;
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
        System.out.println("Order event sent: " + order.getOrderId());
    }
}
