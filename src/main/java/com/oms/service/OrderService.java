package com.oms.service;

import com.oms.entity.Order;
import com.oms.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

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


    // ✅ ADD THIS METHOD
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }
}
