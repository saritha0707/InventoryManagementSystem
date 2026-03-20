package com.oms.service;

import com.oms.entity.Orders;
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

    public Orders placeOrder(Orders order) {

        // 1. Save order in DB
        order.setStatus("CREATED");
        Orders savedOrder = orderRepository.save(order);

        // 2. Send event to Kafka
        kafkaProducerService.sendOrderEvent(savedOrder);

        return savedOrder;
    }


    // ✅ ADD THIS METHOD
    public List<Orders> getAllOrders() {
        return orderRepository.findAll();
    }
}
