package com.oms.controller;

import com.oms.dto.OrderRequestDTO;
import com.oms.dto.OrderResponseDTO;
import com.oms.dto.OrderStatusUpdateRequestDTO;
import com.oms.dto.OrderStatusUpdateResponseDTO;
import com.oms.service.*;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponseDTO createOrder(@Valid @RequestBody OrderRequestDTO dto) {
        return orderService.createOrder(dto);
    }

    @GetMapping
    public List<OrderResponseDTO> getAllOrders() {
        return orderService.getAllOrders();
    }

    @GetMapping("/{id}")
    public OrderResponseDTO getOrderById(@PathVariable int id) {
        return orderService.getOrderById(id);
    }

    @PutMapping("/status/update")
    public OrderStatusUpdateResponseDTO updateOrderStatus(
            @RequestBody OrderStatusUpdateRequestDTO request) {
        return orderService.updateOrderStatus(request.getOrderId(),request.getOrder_status());
    }
}