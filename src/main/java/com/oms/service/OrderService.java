package com.oms.service;

import com.oms.dto.OrderRequestDTO;
import com.oms.dto.OrderResponseDTO;
import com.oms.entity.*;
import com.oms.exception.ResourceNotFoundException;
import com.oms.mapper.OrderMapper;
import com.oms.repository.CustomerRepository;
import com.oms.repository.OrderRepository;
import com.oms.repository.ProductRepository;
import com.oms.repository.WarehouseRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
 //  Better than @Autowired
public class OrderService {

    @Autowired
 private OrderRepository orderRepository;
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

    @Transactional
    public OrderResponseDTO createOrder(OrderRequestDTO dto) {

        log.info("Creating order with {} items", dto.getItems().size());

        //  FIX 1: Strong validation
        validateCustomerOrGuest(dto);

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

            Product product = productRepository.findById(itemDTO.getProductId()).orElseThrow(() -> new ResourceNotFoundException("Product not found: " + itemDTO.getProductId()));

            Warehouse warehouse = warehouseRepository.findById(itemDTO.getWarehouseId()).orElseThrow(() -> new ResourceNotFoundException("Warehouse not found: " + itemDTO.getWarehouseId()));

            OrderItem item = new OrderItem();
            item.setProduct(product);
            item.setWarehouse(warehouse);
            item.setQuantity(itemDTO.getQuantity());
            item.setPrice(product.getPrice());
            item.setOrder(order);

            return item;

        }).collect(Collectors.toList());

        order.setOrderItems(orderItems);

        //  FIX 4: Calculate total safely
        BigDecimal totalAmount = orderItems.stream().map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()))).reduce(BigDecimal.ZERO, BigDecimal::add);

        order.setTotalAmount(totalAmount);

        //  FIX 5: Save FIRST (important for consistency)
        Orders savedOrder = orderRepository.save(order);

        log.info("Order saved successfully with ID: {}", savedOrder.getOrderId());

        //  FIX 6: Kafka AFTER DB commit (basic safe approach)
//        sendKafkaEventSafely(savedOrder);

        return orderMapper.mapToResponseDTO(savedOrder);
    }

    //  FIX 7: Extracted Kafka logic (clean + reusable)
   /* private void sendKafkaEventSafely(Orders order) {
        try {
            producerService.sendOrderCreatedEvent(order);
            log.info("Kafka event sent for orderId={}", order.getOrderId());
        } catch (Exception e) {
            log.error("Kafka publishing failed for orderId={}", order.getOrderId(), e);
            // 🔥 Future: Outbox pattern / retry queue
        }
    }*/

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
        if (OrderStatus.CANCELLED.name().equals(order.getStatus())) {
            throw new IllegalStateException("Order already cancelled");
        }
        order.setStatus(OrderStatus.CANCELLED.name());
        Orders updatedOrder = orderRepository.save(order);
        return orderMapper.mapToResponseDTO(updatedOrder);
    }
}