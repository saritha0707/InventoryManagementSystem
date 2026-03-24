package com.oms.service;

import com.oms.dto.OrderItemRequestDTO;
import com.oms.dto.OrderRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, OrderRequestDTO> kafkaTemplate;

    private static final String TOPIC = "order-events";

    public void sendOrderItemDetails(OrderRequestDTO dto) {
        kafkaTemplate.send(TOPIC, dto);
        System.out.println("Item Details event sent: " + dto);
    }
}
