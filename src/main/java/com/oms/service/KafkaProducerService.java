package com.oms.service;

import com.oms.config.KafkaTopicsConfig;
import com.oms.event.InventoryCheckResponseEvent;
import com.oms.event.InventoryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaTopicsConfig kafkaTopicsConfig;

    private static final String Order_TOPIC = "order-events";
    private static final String Inventory_TOPIC = "inventory-events";

    public void publishInventoryCheckResponse(InventoryCheckResponseEvent responseEvent) {
        kafkaTemplate.send(kafkaTopicsConfig.getInventoryCheckResponse(),responseEvent);
        log.info(responseEvent.toString());
    }

    /*
    public void publishInventoryUpdatedEvent(InventoryEvent event) {
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
    }*/
}
