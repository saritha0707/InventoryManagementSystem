package com.oms.service;

import com.oms.dto.InventoryResponseDTO;
import com.oms.dto.OrderRequestDTO;
import com.oms.dto.OrderResponseDTO;
import com.oms.dto.OrderStatusUpdateResponseDTO;
import com.oms.entity.*;
import com.oms.enums.OrderStatus;
import com.oms.enums.PaymentMethod;
import com.oms.event.OrderCreatedEvent;
import com.oms.exception.*;
import com.oms.mapper.OrderMapper;
import com.oms.repository.*;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

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

    @Autowired
    PaymentRepository paymentRepository;

    @Transactional
    public OrderResponseDTO createOrder(OrderRequestDTO dto) {

        if (dto.getPaymentMethod() == PaymentMethod.REFUND) {
            throw new InvalidPaymentMethodException(
                    "Invalid value for paymentMethod. Allowed values: CASH_ON_DELIVERY, ONLINE"
            );
        }
        log.info("Creating order with {} items", dto.getItems().size());

        //  FIX 1: Strong validation
        validateCustomerOrGuest(dto);

        // ✅ NEW FIX: Validate inventory availability BEFORE creating order
        validateInventoryAvailability(dto);

        Orders order = new Orders();
        order.setStatus(OrderStatus.CREATED.name());

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

            List<InventoryResponseDTO> response = inventoryService.getProductAvailability(itemDTO.getProductId());
            Integer quantity = response.stream()
                    .filter(i -> i.getWarehouseId() == itemDTO.getWarehouseId())
                    .map(InventoryResponseDTO::getQuantity)
                    .findFirst()
                    .orElse(0);   // default if not found
            //get details from inventory service and then call below lines
            if(!inventoryService.isProductAvailable(itemDTO.getProductId(), itemDTO.getWarehouseId(),itemDTO.getQuantity())) {
                throw new InsufficientStockException(itemDTO.getProductId(),
                        quantity,
                        itemDTO.getQuantity());
            }
          //orElseThrow(() -> new ResourceNotFoundException("Product not found: " + itemDTO.getProductId()))
            Product product = productRepository.findById(itemDTO.getProductId()).orElseThrow(() -> new ResourceNotFoundException("Product not found: " + itemDTO.getProductId()));

            Warehouse warehouse = warehouseRepository.findById(itemDTO.getWarehouseId()).orElseThrow(() -> new ResourceNotFoundException("Warehouse not found: " + itemDTO.getWarehouseId()));

            OrderItem item = new OrderItem();
            item.setProduct(product);
            item.setWarehouse(warehouse);
            item.setQuantity(itemDTO.getQuantity());
            item.setPrice(product.getPrice());
            item.setInventoryStatus("Available");
            item.setAvailableQuantity(quantity);
            item.setOrder(order);
             //We have already invoked this method during Kafka processing, so it has been commented out here to prevent multiple reductions of the product.
            //below method is trying to reduce inventory for items
            //inventoryService.reduceInventory(itemDTO.getProductId(),itemDTO.getWarehouseId(),itemDTO.getQuantity());
            return item;

        }).collect(Collectors.toList());

        order.setOrderItems(orderItems);

        //  FIX 4: Calculate total safely
        BigDecimal totalAmount = orderItems.stream().map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()))).reduce(BigDecimal.ZERO, BigDecimal::add);

        order.setTotalAmount(totalAmount);

        //update Payment Table
        Payment payment = new Payment();
        payment.setAmount(totalAmount);
        payment.setOrder(order);
        payment.setPaymentMethod(dto.getPaymentMethod());
        if(dto.getPaymentMethod() == PaymentMethod.ONLINE)
        payment.setPaymentStatus("PAID");
        else if (dto.getPaymentMethod() == PaymentMethod.CASH_ON_DELIVERY)
            payment.setPaymentStatus("PENDING");
        order.setPayments(List.of(payment));

        //  FIX 5: Save FIRST (important for consistency)
        Orders savedOrder = orderRepository.save(order);

        //Update OrderStatusHistory Table
        OrdersStatusHistory statusHistory = new OrdersStatusHistory();
        statusHistory.setOrder(savedOrder);
        statusHistory.setStatus(savedOrder.getStatus());
        statusHistory.setChangedBy("USER");
        OrdersStatusHistory savedstatusHistory = statusHistoryRepository.save(statusHistory);

        log.info("Order saved successfully with ID: {}", savedOrder.getOrderId());

        //  FIX 6: Kafka AFTER DB commit (basic safe approach)
        //Update Inventory
        sendKafkaEventSafely(savedOrder);

        OrderResponseDTO response = orderMapper.mapToResponseDTO(savedOrder);
        return new ResponseEntity<>(response, HttpStatus.CREATED).getBody();
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
                        .warehouseId(item.getWarehouse() != null ? item.getWarehouse().getWarehouseId() : null)
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
                throw new CustomerOrGuestValidationException("Either customerId OR guestName & guestEmail must be provided");
            }
        }
    }

    // ✅ NEW: Validate inventory availability before order creation
    private void validateInventoryAvailability(OrderRequestDTO dto) {
        if (dto.getItems() == null || dto.getItems().isEmpty()) {
            throw new IllegalArgumentException("Order must have at least one item");
        }

        for (var itemDTO : dto.getItems()) {
            // Validate product exists
            Product product = productRepository.findById(itemDTO.getProductId())
                    .orElseThrow(() -> new ResourceNotFoundException(
                            "Product not found: " + itemDTO.getProductId()));

            // Validate warehouse exists
            Warehouse warehouse = warehouseRepository.findById(itemDTO.getWarehouseId())
                    .orElseThrow(() -> new ResourceNotFoundException(
                            "Warehouse not found: " + itemDTO.getWarehouseId()));

            // ✅ NEW: Validate inventory exists and has sufficient quantity
            var inventory = inventoryService.getInventoryOrThrow(itemDTO.getProductId(), itemDTO.getWarehouseId());

            if (inventory.getQuantity() <= 0) {
                throw new InsufficientStockException(
                        itemDTO.getProductId(),
                        inventory.getQuantity(),
                        itemDTO.getQuantity()
                );
            }

            if (inventory.getQuantity() < itemDTO.getQuantity()) {
                throw new InsufficientStockException(
                        itemDTO.getProductId(),
                        inventory.getQuantity(),
                        itemDTO.getQuantity()
                );
            }

            log.info("Inventory validated: productId={}, warehouseId={}, available={}, requested={}",
                    itemDTO.getProductId(), itemDTO.getWarehouseId(),
                    inventory.getQuantity(), itemDTO.getQuantity());
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
        if (OrderStatus.CANCELLED.name().equals(order.getStatus())) {
            throw new InvalidOrderStatusException("Order already cancelled");
        }
        order.setStatus(OrderStatus.CANCELLED.name());
        Orders updatedOrder = orderRepository.save(order);

        // ✅ Restore inventory for each item (gracefully handle missing inventory)
        order.getOrderItems()
                .stream()
                .forEach(item -> {
                    boolean restored = inventoryService.restoreInventoryIfExists(
                            item.getProduct().getProductId(),
                            item.getWarehouse().getWarehouseId(),
                            item.getQuantity()
                    );
                    if (!restored) {
                        log.warn("Could not restore inventory for cancelled order {}: productId={}, warehouseId={}",
                                orderId, item.getProduct().getProductId(), item.getWarehouse().getWarehouseId());
                    }
                });
        List<Payment> payments = paymentRepository.findByOrder(updatedOrder);

        if (!payments.isEmpty()) {
            Payment payment = payments.get(0);

            if (payment.getPaymentMethod() == PaymentMethod.ONLINE) {
                payment.setPaymentMethod(PaymentMethod.REFUND);
                paymentRepository.save(payment);
            }

        }
        return orderMapper.mapToResponseDTO(updatedOrder);
    }

    public OrderStatusUpdateResponseDTO updateOrderStatus(int orderId, String status) {

        Orders order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        // ✅ Convert input status → Enum
        OrderStatus newStatus;
        try {
            newStatus = OrderStatus.valueOf(status.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new InvalidOrderStatusException("Not a valid status");
        }

        // ✅ Convert DB status (String → Enum)
        OrderStatus currentStatus;
        try {
            currentStatus = OrderStatus.valueOf(order.getStatus().toUpperCase());
        } catch (Exception e) {
            throw new InvalidOrderStatusException("Invalid status in DB: " + order.getStatus());
        }

        // ✅ Prevent same status update
        if (currentStatus == newStatus) {
            throw new InvalidOrderStatusException(
                    "Order is already in "+currentStatus+" status."
            );
        }

        // ✅ Handle CANCELLED separately
        if (newStatus == OrderStatus.CANCELLED) {

            if (currentStatus == OrderStatus.SHIPPED ||
                    currentStatus == OrderStatus.CANCELLED) {

                throw new InvalidOrderStatusException(
                        "Order cannot be CANCELLED when status is " + currentStatus

                );
            }

            cancelOrder(orderId);
        }

        // ✅ Forward flow validation
        if (!isValidTransition(currentStatus, newStatus)) {
            throw new InvalidOrderStatusException(
                    "Invalid status transition from " + currentStatus + " to " + newStatus
            );
        }

        // ✅ Update status
        order.setStatus(newStatus.name());
        orderRepository.save(order);

        // ✅ Save history
        OrdersStatusHistory history = new OrdersStatusHistory();
        history.setOrder(order);
        history.setStatus(newStatus.name());
        history.setChangedBy("USER");
        statusHistoryRepository.save(history);

        // ✅ Response
        OrderStatusUpdateResponseDTO response = new OrderStatusUpdateResponseDTO();
        response.setMessage("Order status updated successfully");
        response.setId(orderId);
        Optional<Orders> orders = orderRepository.findById(orderId);
        response.setStatus(orders.get().getStatus());

        return response;
    }

    // ✅ New method: Check and update status conditionally (for Kafka consumer)
    @Transactional
    public boolean updateOrderStatusIfNotAlready(int orderId, OrderStatus targetStatus) {
        Orders order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        // If already in target status, return false (no update needed)
        if (targetStatus.name().equals(order.getStatus())) {
            return false;
        }

        // Update the status
        OrderStatusUpdateResponseDTO result = updateOrderStatus(orderId, targetStatus.name());
        return result != null;
    }

    // ✅ NEW: Update order items with inventory processing results
    @Transactional
    public void updateOrderItemsWithInventoryResults(int orderId, List<com.oms.event.InventoryUpdatedEvent.ItemResult> itemResults) {
        Orders order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        if (itemResults == null || itemResults.isEmpty()) {
            log.warn("No item results provided for orderId={}", orderId);
            return;
        }

        // ✅ Map item results by productId for quick lookup
        Map<Integer, com.oms.event.InventoryUpdatedEvent.ItemResult> resultsByProductId = itemResults.stream()
                .collect(Collectors.toMap(
                        com.oms.event.InventoryUpdatedEvent.ItemResult::getProductId,
                        result -> result,
                        (existing, replacement) -> existing
                ));

        // ✅ Update each order item with its inventory status
        for (OrderItem item : order.getOrderItems()) {
            com.oms.event.InventoryUpdatedEvent.ItemResult result = resultsByProductId.get(item.getProduct().getProductId());

            if (result != null) {
                item.setInventoryStatus(result.getStatus());
                item.setAvailableQuantity(result.getAvailableQuantity());
                log.info("Updated orderItem {} with status={}, availableQty={}",
                        item.getOrderItemId(), result.getStatus(), result.getAvailableQuantity());
            } else {
                log.warn("No inventory result found for productId={} in order {}",
                        item.getProduct().getProductId(), orderId);
            }
        }

        // ✅ Save the updated items
        orderRepository.save(order);
    }

    private boolean isValidTransition(OrderStatus current, OrderStatus next) {

        switch (current) {

            case CREATED:
                return next == OrderStatus.PARTIAL ||
                        next == OrderStatus.PROCESSED ||
                        next == OrderStatus.CANCELLED;

            case PARTIAL:
                return next == OrderStatus.PROCESSED ||
                        next == OrderStatus.SHIPPED ||
                        next == OrderStatus.CANCELLED;

            case PROCESSED:
                return next == OrderStatus.SHIPPED ||
                        next == OrderStatus.CANCELLED;

            case SHIPPED:
                return false; // no further transitions

            case CANCELLED:
                return false; // terminal state

            default:
                return false;
        }
    }
}

