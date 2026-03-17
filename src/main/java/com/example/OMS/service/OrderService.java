package com.example.OMS.service;

import com.example.OMS.entity.Order;
import com.example.OMS.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    public Order placeOrder(Order order) {

        // 1. Save order in DB
        order.setStatus("CREATED");
        Order savedOrder = orderRepository.save(order);

        // 2. Send event to Kafka
        kafkaProducerService.sendOrderEvent(savedOrder);

        return savedOrder;
    }
}
