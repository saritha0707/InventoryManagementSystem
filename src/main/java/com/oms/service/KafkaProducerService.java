package com.oms.service;

import com.oms.dto.InventoryRequestDTO;
import com.oms.dto.OrderItemRequestDTO;
import com.oms.dto.OrderRequestDTO;
import com.oms.event.InventoryUpdatedEvent;
import com.oms.event.OrderCreatedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String Order_TOPIC = "order-events";
    private static final String Inventory_TOPIC = "inventory-event";

    public void sendOrderItemDetails(OrderRequestDTO dto) {
        kafkaTemplate.send(Order_TOPIC, dto);
        System.out.println("Item Details event sent: " + dto);
    }

    public void sendInventoryItemDetails(InventoryRequestDTO inv) {
        kafkaTemplate.send(Inventory_TOPIC, inv);
        System.out.println("Inventory Response event sent: " + inv);
    }

    public void publishInventoryUpdatedEvent(InventoryUpdatedEvent event) {
        kafkaTemplate.send(Inventory_TOPIC, event);
        System.out.println("InventoryUpdatedEvent sent: eventId=" + event.getEventId() +
                ", productId=" + event.getProductId() +
                ", status=" + event.getStatus());
    }

    public void publishOrderCreatedEvent(OrderCreatedEvent event) {
        kafkaTemplate.send(Order_TOPIC, event);
        System.out.println("OrderCreatedEvent sent: eventId=" + event.getEventId() +
                ", orderId=" + event.getOrderId() +
                ", status=" + event.getStatus());
    }
}
