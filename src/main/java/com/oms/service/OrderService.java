package com.oms.service;

import com.oms.dto.*;
import com.oms.entity.*;
import com.oms.exception.InvalidOrderStateException;
import com.oms.exception.ResourceNotFoundException;
import com.oms.repository.OrderRepository;
import com.oms.repository.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderService {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private KafkaProducerService producerService;
    // Create Order (Multi Product)
    public OrderResponseDTO createOrder(OrderRequestDTO dto) {

        log.info("Creating order with {} items", dto.getItems().size());

        Orders order = new Orders();

        order.setStatus(String.valueOf(OrderStatus.CREATED));
        //send message to kafka topic
        producerService.sendOrderItemDetails(dto);


        // Convert DTO → OrderItems
        List<OrderItem> orderItems = dto.getItems().stream().map(itemDTO -> {


            Product product = productRepository.findById(itemDTO.getProductId())
                    .orElseThrow(() ->
                            new ResourceNotFoundException("Product not found with id: " + itemDTO.getProductId())
                    );
            OrderItem item = new OrderItem();
            item.setProduct(product);
            item.setQuantity(itemDTO.getQuantity());
            item.setPrice(product.getPrice());
            item.setOrder(order);

            return item;

        }).collect(Collectors.toList());

        order.setOrderItems(orderItems);

        // Calculate total amount
        BigDecimal totalAmount = orderItems.stream()
                .map(item -> item.getPrice()
                        .multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        order.setTotalAmount(totalAmount);

        //  Save Order (cascade saves items)
        Orders savedOrder = orderRepository.save(order);

        log.info("Order created successfully with id: {}", savedOrder.getOrderId());

        return mapToResponseDTO(savedOrder);
    }

    // Get All Orders
    public List<OrderResponseDTO> getAllOrders() {

        return orderRepository.findAll()
                .stream()
                .map(this::mapToResponseDTO)
                .collect(Collectors.toList());
    }

    //  Get Order By ID
    public OrderResponseDTO getOrderById(int id) {

        Orders order = orderRepository.findById(Math.toIntExact(id))
                .orElseThrow(() ->
                        new ResourceNotFoundException("Order not found with id: " + id));

        return mapToResponseDTO(order);
    }

    // Cancel Order
    public OrderResponseDTO cancelOrder(int orderId) {

        Orders order = orderRepository.findById(Math.toIntExact(orderId))
                .orElseThrow(() ->
                        new ResourceNotFoundException("Order not found with id: " + orderId));

        order.setStatus(String.valueOf(OrderStatus.CANCELLED));

        Orders updatedOrder = orderRepository.save(order);

        return mapToResponseDTO(updatedOrder);
    }

    // Mapping Method (Orders → OrderResponseDTO)
    private OrderResponseDTO mapToResponseDTO(Orders order) {

        OrderResponseDTO dto = new OrderResponseDTO();

        dto.setId((int) order.getOrderId());
        dto.setStatus(order.getStatus());
        dto.setTotalAmount(order.getTotalAmount());

        List<OrderItemResponseDTO> items = order.getOrderItems().stream().map(item -> {
            OrderItemResponseDTO itemDTO = new OrderItemResponseDTO();
            itemDTO.setProductId((int) item.getProduct().getProductId());
            itemDTO.setQuantity(item.getQuantity());
            itemDTO.setPrice(item.getPrice());
            return itemDTO;
        }).collect(Collectors.toList());

        dto.setItems(items);

        return dto;
    }
}