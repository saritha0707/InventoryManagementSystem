package com.oms.service;

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
    private static final String Inventory_TOPIC = "inventory-events";

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
