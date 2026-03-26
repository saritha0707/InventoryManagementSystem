package com.oms.service;

import com.oms.dto.OrderRequestDTO;
import com.oms.dto.OrderResponseDTO;
import com.oms.dto.OrderStatusUpdateResponseDTO;
import com.oms.entity.*;
import com.oms.event.OrderCreatedEvent;
import com.oms.exception.InvalidOrderStatusException;
import com.oms.exception.ResourceNotFoundException;
import com.oms.mapper.OrderMapper;
import com.oms.repository.*;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.oms.entity.OrderStatus.*;

@Service
@Slf4j
public class OrderService {

    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    OrderStatusHistoryRepository statusHistoryRepository;
    @Autowired
    private ProductRepository productRepository;
    @Autowired
    private WarehouseRepository warehouseRepository;
    @Autowired
    private CustomerRepository customerRepository;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private KafkaProducerService producerService;

    @Autowired
    private InventoryService inventoryService;

    @Transactional
    public OrderResponseDTO createOrder(OrderRequestDTO dto) {

        log.info("Creating order with {} items", dto.getItems().size());

        //  FIX 1: Strong validation
        validateCustomerOrGuest(dto);

        Orders order = new Orders();
        order.setStatus(CREATED.name());

        //  FIX 2: Customer vs Guest handling
        if (dto.getCustomerId() != null) {
            Customer customer = customerRepository.findById(dto.getCustomerId()).orElseThrow(() -> new ResourceNotFoundException("Customer not found"));
            order.setCustomer(customer);
        } else {
            order.setGuestName(dto.getGuestName());
            order.setGuestEmail(dto.getGuestEmail());
            order.setGuestPhone(dto.getGuestPhone());
        }

        //  FIX 3: Map items with warehouse (ER aligned)
        List<OrderItem> orderItems = dto.getItems().stream().map(itemDTO -> {

            // we need to send message to kafka topic and it should be consumed by inventory service
            producerService.sendOrderItemDetails(dto);

            //get details from inventory service and then call below lines
            Product product = productRepository.findById(itemDTO.getProductId()).orElseThrow(() -> new ResourceNotFoundException("Product not found: " + itemDTO.getProductId()));

            Warehouse warehouse = warehouseRepository.findById(itemDTO.getWarehouseId()).orElseThrow(() -> new ResourceNotFoundException("Warehouse not found: " + itemDTO.getWarehouseId()));

            OrderItem item = new OrderItem();
            item.setProduct(product);
            item.setWarehouse(warehouse);
            item.setQuantity(itemDTO.getQuantity());
            item.setPrice(product.getPrice());
            item.setOrder(order);

            //below method is trying to reduce inventory for items
            inventoryService.reduceInventory(itemDTO.getProductId(),itemDTO.getWarehouseId(),itemDTO.getQuantity());
            return item;

        }).collect(Collectors.toList());

        order.setOrderItems(orderItems);

        //reduce inventory

        //  FIX 4: Calculate total safely
        BigDecimal totalAmount = orderItems.stream().map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()))).reduce(BigDecimal.ZERO, BigDecimal::add);

        order.setTotalAmount(totalAmount);

        //  FIX 5: Save FIRST (important for consistency)
        Orders savedOrder = orderRepository.save(order);
        OrdersStatusHistory statusHistory = new OrdersStatusHistory();
        statusHistory.setOrder(savedOrder);
        statusHistory.setStatus(savedOrder.getStatus());
        statusHistory.setChangedBy("User");
        OrdersStatusHistory savedstatusHistory = statusHistoryRepository.save(statusHistory);

        log.info("Order saved successfully with ID: {}", savedOrder.getOrderId());

        //  FIX 6: Kafka AFTER DB commit (basic safe approach)
        sendKafkaEventSafely(savedOrder);

        return orderMapper.mapToResponseDTO(savedOrder);
    }

    //  FIX 7: Extracted Kafka logic (clean + reusable)
    private void sendKafkaEventSafely(Orders order) {
        try {
            OrderCreatedEvent event = buildOrderCreatedEvent(order);
            producerService.publishOrderCreatedEvent(event);
            log.info("Kafka event sent for orderId={}", order.getOrderId());
        } catch (Exception e) {
            log.error("Kafka publishing failed for orderId={}", order.getOrderId(), e);
            // 🔥 Future: Outbox pattern / retry queue
        }
    }

    /**
     * Builds OrderCreatedEvent from saved order entity
     */
    private OrderCreatedEvent buildOrderCreatedEvent(Orders order) {
        // Build item events
        List<OrderCreatedEvent.OrderItemEvent> itemEvents = order.getOrderItems().stream()
                .map(item -> OrderCreatedEvent.OrderItemEvent.builder()
                        .orderItemId((long) item.getOrderItemId())
                        .productId(item.getProduct().getProductId())
                        .productName(item.getProduct().getProductName())
                        .quantity(item.getQuantity())
                        .price(item.getPrice())
                        .warehouseName(item.getWarehouse() != null ? item.getWarehouse().getWarehouseName() : null)
                        .build())
                .collect(Collectors.toList());

        // Build main event
        return OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId((long) order.getOrderId())
                .customerId(order.getCustomer() != null ? order.getCustomer().getCustomerId() : null)
                .customerName(order.getCustomer() != null ? order.getCustomer().getName() : null)
                .guestName(order.getGuestName())
                .guestEmail(order.getGuestEmail())
                .guestPhone(order.getGuestPhone())
                .totalAmount(order.getTotalAmount())
                .status(order.getStatus())
                .timestamp(LocalDateTime.now())
                .items(itemEvents)
                .build();
    }

    //  FIX 8: Strong validation logic
    private void validateCustomerOrGuest(OrderRequestDTO dto) {

        if (dto.getCustomerId() == null) {
            if (dto.getGuestName() == null || dto.getGuestEmail() == null) {
                throw new IllegalArgumentException("Either customerId OR guestName & guestEmail must be provided");
            }
        }
    }

    // Get All Orders
    public List<OrderResponseDTO> getAllOrders() {
        return orderRepository.findAll().stream().map(orderMapper::mapToResponseDTO).collect(Collectors.toList());
    }

    // Get Order By ID
    public OrderResponseDTO getOrderById(int id) {
        Orders order = orderRepository.findById(id).orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + id));

        return orderMapper.mapToResponseDTO(order);
    }

    // Cancel Order
    public OrderResponseDTO cancelOrder(int orderId) {

        Orders order = orderRepository.findById(orderId).orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + orderId));

        //  FIX 9: Prevent invalid state transition
        if (CANCELLED.name().equals(order.getStatus())) {
            throw new IllegalStateException("Order already cancelled");
        }
        order.setStatus(CANCELLED.name());
        Orders updatedOrder = orderRepository.save(order);
        order.getOrderItems()
                .stream()
                .forEach(item -> inventoryService.restoreInventory(
                        item.getProduct().getProductId(),
                        item.getWarehouse().getWarehouseId(),
                        item.getQuantity()
                ));

        return orderMapper.mapToResponseDTO(updatedOrder);
    }

    //String working on error scenarios - WIP
    public OrderStatusUpdateResponseDTO updateOrderStatus(int orderId, String status) {
        Orders order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found"));
        String orderStatus = "";
        try {
            orderStatus = OrderStatus.valueOf(status.toUpperCase()).name();
        } catch (IllegalArgumentException e) {
            throw new InvalidOrderStatusException("Not a valid status");
        }
        if (orderStatus == OrderStatus.CANCELLED.name()) {
            if(order.getStatus() == "SHIPPED" || order.getStatus() == "CANCELLED") {
                String message = "Order status is already" + order.getStatus();
                throw new InvalidOrderStatusException(message);
            }
            else
                cancelOrder(orderId);
        }

        // Set validated status
        order.setStatus(orderStatus);
        orderRepository.save(order);

        // Save status history
        OrdersStatusHistory statusHistory = new OrdersStatusHistory();
        statusHistory.setOrder(order);
        statusHistory.setStatus(orderStatus);
        statusHistory.setChangedBy("USER");
        statusHistoryRepository.save(statusHistory);

        // Response
        OrderStatusUpdateResponseDTO response = new OrderStatusUpdateResponseDTO();
        response.setMessage("Order status updated successfully");
        return response;
    }
}