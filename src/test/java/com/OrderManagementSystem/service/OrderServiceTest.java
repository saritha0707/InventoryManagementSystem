package com.OrderManagementSystem.service;

import com.oms.dto.*;
import com.oms.entity.*;
import com.oms.enums.PaymentMethod;
import com.oms.exception.*;
import com.oms.mapper.OrderMapper;
import com.oms.repository.*;
import com.oms.service.InventoryService;
import com.oms.service.KafkaProducerService;
import com.oms.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(org.mockito.junit.jupiter.MockitoExtension.class)
public class OrderServiceTest {

    @InjectMocks
   private OrderService orderService;

    @Mock
    private OrderRepository orderRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private WarehouseRepository warehouseRepository;
    @Mock
    private CustomerRepository customerRepository;
    @Mock
    private OrderMapper orderMapper;
    @Mock
    private KafkaProducerService producerService;
    @Mock
    private InventoryService inventoryService;
    @Mock
    private OrderStatusHistoryRepository statusHistoryRepository;
    @Mock
    private PaymentRepository paymentRepository;

    private OrderRequestDTO requestDTO;
    private Product product;
    private Warehouse warehouse;
    private Customer customer;

    @BeforeEach
    void setup() {
        product = new Product();
        product.setProductId(1);
        product.setPrice(BigDecimal.valueOf(100));

        warehouse = new Warehouse();
        warehouse.setWarehouseId(1);

        customer = new Customer();
        customer.setCustomerId(1);
        customer.setName("Test User");

        OrderItemRequestDTO itemDTO = new OrderItemRequestDTO();
        itemDTO.setProductId(1);
        itemDTO.setWarehouseId(1);
        itemDTO.setQuantity(2);

        requestDTO = new OrderRequestDTO();
        requestDTO.setItems(List.of(itemDTO));
        requestDTO.setCustomerId(1);
        requestDTO.setPaymentMethod(PaymentMethod.ONLINE);
    }

    @Test //Success -Customer order
    void shouldCreateOrderSuccessfully_withCustomer() {

        when(customerRepository.findById(1)).thenReturn(Optional.of(customer));
        when(productRepository.findById(1)).thenReturn(Optional.of(product));
        when(warehouseRepository.findById(1)).thenReturn(Optional.of(warehouse));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArguments()[0]);
        when(orderMapper.mapToResponseDTO(any())).thenReturn(new OrderResponseDTO());

        OrderResponseDTO response = orderService.createOrder(requestDTO);

        assertNotNull(response);
        verify(orderRepository).save(any());
        verify(producerService).publishOrderCreatedEvent(any());
    }

    @Test // Success - Guest Order
    void shouldCreateOrderSuccessfully_withGuest() {

        requestDTO.setCustomerId(null);
        requestDTO.setGuestName("Guest");
        requestDTO.setGuestEmail("guest@test.com");

        when(productRepository.findById(1)).thenReturn(Optional.of(product));
        when(warehouseRepository.findById(1)).thenReturn(Optional.of(warehouse));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArguments()[0]);
        when(orderMapper.mapToResponseDTO(any())).thenReturn(new OrderResponseDTO());

        OrderResponseDTO response = orderService.createOrder(requestDTO);

        assertNotNull(response);
    }
    @Test //Validation Failure (No customer & no guest)
    void shouldThrowException_whenCustomerAndGuestMissing() {

        requestDTO.setCustomerId(null);

        assertThrows(CustomerOrGuestValidationException.class,
                () -> orderService.createOrder(requestDTO));
    }

    @Test //Product Not Found
    void shouldThrowException_whenProductNotFound() {

        when(customerRepository.findById(1)).thenReturn(Optional.of(customer));
        when(productRepository.findById(1)).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class,
                () -> orderService.createOrder(requestDTO));
    }

    @Test //Warehouse Not Found
    void shouldThrowException_whenWarehouseNotFound() {

        when(customerRepository.findById(1)).thenReturn(Optional.of(customer));
        when(productRepository.findById(1)).thenReturn(Optional.of(product));
        when(warehouseRepository.findById(1)).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class,
                () -> orderService.createOrder(requestDTO));
    }

//    @Test //Kafka Failure Should Not Break Order
    void shouldNotFail_whenKafkaFails() {

        when(customerRepository.findById(1)).thenReturn(Optional.of(customer));
        when(productRepository.findById(1)).thenReturn(Optional.of(product));
        when(warehouseRepository.findById(1)).thenReturn(Optional.of(warehouse));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArguments()[0]);
        when(orderMapper.mapToResponseDTO(any())).thenReturn(new OrderResponseDTO());

        doThrow(new RuntimeException("Kafka down"))
                .when(producerService).publishOrderCreatedEvent(any());

        assertDoesNotThrow(() -> orderService.createOrder(requestDTO));
    }

    @Test //Success -getOrderById()
    void shouldReturnOrderById() {

        Orders order = new Orders();

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));
        when(orderMapper.mapToResponseDTO(order)).thenReturn(new OrderResponseDTO());

        OrderResponseDTO response = orderService.getOrderById(1);

        assertNotNull(response);
    }

    @Test //Not Found
    void shouldThrowException_whenOrderNotFound() {

        when(orderRepository.findById(1)).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class,
                () -> orderService.getOrderById(1));
    }

    @Test //Success - cancelOrder()

    void shouldCancelOrderSuccessfully() {

        Orders order = new Orders();
        order.setStatus("CREATED");

        OrderItem item = new OrderItem();
        item.setProduct(product);
        item.setWarehouse(warehouse);
        item.setQuantity(2);

        order.setOrderItems(List.of(item));

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));
        when(orderRepository.save(any())).thenReturn(order);
        when(paymentRepository.findByOrder(order)).thenReturn(new ArrayList<>());
        when(orderMapper.mapToResponseDTO(any()))
                .thenReturn(new OrderResponseDTO());

        OrderResponseDTO response = orderService.cancelOrder(1);
        assertNotNull(response);

        verify(inventoryService).restoreInventory(1, 1, 2);
    }

    @Test //Already Cancelled
    void shouldThrowException_whenAlreadyCancelled() {

        Orders order = new Orders();
        order.setStatus("CANCELLED");

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));

        assertThrows(InvalidOrderStatusException.class,
                () -> orderService.cancelOrder(1));
    }

    @Test //Valid Transition -updateOrderStatus()
    void shouldUpdateStatusSuccessfully() {

        Orders order = new Orders();
        order.setStatus("CREATED");

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));

        OrderStatusUpdateResponseDTO response =
                orderService.updateOrderStatus(1, "PROCESSED");

        assertEquals("Order status updated successfully", response.getMessage());
    }

    @Test //Invalid Transition
    void shouldThrowException_forInvalidTransition() {

        Orders order = new Orders();
        order.setStatus("SHIPPED");

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));

        assertThrows(InvalidOrderStatusException.class,
                () -> orderService.updateOrderStatus(1, "CREATED"));
    }

    @Test // SameStatus
    void shouldThrowException_whenSameStatus() {

        Orders order = new Orders();
        order.setStatus("CREATED");

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));

        assertThrows(InvalidOrderStatusException.class,
                () -> orderService.updateOrderStatus(1, "CREATED"));
    }

    @Test //Invalid Status String
    void shouldThrowException_whenInvalidStatusString() {

        Orders order = new Orders();
        order.setStatus("CREATED");

        when(orderRepository.findById(1)).thenReturn(Optional.of(order));

        assertThrows(InvalidOrderStatusException.class,
                () -> orderService.updateOrderStatus(1, "INVALID"));
    }

    @Test // Get all orders
    void shouldReturnAllOrders() {

        Orders order = new Orders();

        when(orderRepository.findAll()).thenReturn(List.of(order));
        when(orderMapper.mapToResponseDTO(order)).thenReturn(new OrderResponseDTO());

        List<OrderResponseDTO> response = orderService.getAllOrders();

        assertEquals(1, response.size());
    }
}