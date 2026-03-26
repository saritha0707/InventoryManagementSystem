package com.oms.service;

import com.oms.dto.InventoryResponse;
import com.oms.dto.OrderItemRequestDTO;
import com.oms.dto.OrderRequestDTO;
import com.oms.entity.Product;
import com.oms.repository.WarehouseRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

public class KafkaConsumerService {
    @Autowired
    private OrderItemRequestDTO orderItem;
    @Autowired
    private InventoryService inventoryService;
    @Autowired
    private InventoryResponse productList;
    @Autowired
    private WarehouseRepository warehouseRepository;

    @KafkaListener(topics = "order-events", groupId = "order-group")
    public void consume(OrderRequestDTO dto) {
        System.out.println("Received order: " + dto);
      //  productList = (InventoryResponse) inventoryService.getProductAvailability(dto.getProductId(),warehouseRepository.findWarehouseIdByName(dto.getWarehousename()),dto.getQuantity());
    }

    @KafkaListener(topics = "inventory-event", groupId = "inventory-group")
    public void consumeInventory(InventoryResponse dto) {
        System.out.println("Received inventory: " + dto);
    }
}


