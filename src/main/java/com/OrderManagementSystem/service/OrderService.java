package com.OrderManagementSystem.service;

import com.OrderManagementSystem.entity.OrderDetails;
import com.OrderManagementSystem.modal.OrderDAO;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderService {

    private final OrderDAO repository;

    public OrderService(OrderDAO repository){
        this.repository = repository;
    }

    public OrderDetails createOrder(OrderDetails order){
        return repository.save(order);
    }

    public List<OrderDetails> getOrders(){
        return repository.findAll();
    }
}
