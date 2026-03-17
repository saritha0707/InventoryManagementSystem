package com.OrderManagementSystem.controller;

import com.OrderManagementSystem.entity.OrderDetails;
import com.OrderManagementSystem.service.OrderService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/order")
public class OrderController {
    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    public OrderDetails createUser(@RequestBody OrderDetails order) {
        return service.createOrder(order);
    }

    @GetMapping
    public List<OrderDetails> getUsers() {
        return service.getOrders();
    }
}
