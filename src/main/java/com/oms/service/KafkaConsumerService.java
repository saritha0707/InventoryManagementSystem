package com.oms.service;

import com.oms.dto.InventoryResponseDTO;
import com.oms.dto.OrderItemRequestDTO;
import com.oms.entity.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

public class KafkaConsumerService {
    @Autowired
    private OrderItemRequestDTO orderItem;
    private InventoryService inventoryService;
    private InventoryResponseDTO productList;

    @KafkaListener(topics = "order-events", groupId = "order-group")
    public void consume(OrderItemRequestDTO dto) {
        System.out.println("Received order: " + dto);
        productList = (InventoryResponseDTO) inventoryService.getProductAvailability(dto.getProductId());
    }
}
