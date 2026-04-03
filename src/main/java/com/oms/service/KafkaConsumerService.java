package com.oms.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.entity.EventLog;
import com.oms.entity.Inventory;
import com.oms.entity.Warehouse;
import com.oms.enums.OrderStatus;
import com.oms.event.BaseEvent;
import com.oms.event.InventoryUpdatedEvent;
import com.oms.event.OrderCreatedEvent;
import com.oms.exception.InsufficientStockException;
import com.oms.repository.EventLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
public class KafkaConsumerService {

    private final InventoryService inventoryService;
    private final OrderService orderService;
    private final EventLogRepository eventLogRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(InventoryService inventoryService,
                                OrderService orderService,
                                EventLogRepository eventLogRepository,
                                KafkaProducerService kafkaProducerService,
                                ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.orderService = orderService;
        this.eventLogRepository = eventLogRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${kafka.topics.order-events:order-events}",
            groupId = "${kafka.consumer.group-id:order-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Retryable(
            retryFor = {Exception.class},
            exclude = {InsufficientStockException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, multiplier = 2)
    )
    public void consumeOrderCreatedEvent(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {

        log.info("Received OrderCreatedEvent: eventId={}, orderId={}",
                event.getEventId(), event.getOrderId());

        try {
            if(event.getItems() == null || event.getItems().isEmpty()) {
                log.error("Invalid event received");
                acknowledgment.acknowledge();
                return;
            }
            if (isInvalidOrDuplicate(event, acknowledgment)) {
                return;
            }

            // ✅ Create log
            EventLog eventLog = createEventLog(event, "PROCESSING");

            boolean hasFailures = false;

            // ✅ Process items (ONLY ONCE)
            for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {

                if (item.getProductId() == null || item.getWarehouseId() == null) {
                    log.error("Invalid item: productId or warehouseId is null");
                    publishInventoryUpdatedEvent(event, item, null, 0, "INVALID_INPUT");
                    continue;
                }

                if (item.getQuantity() <= 0) {
                    log.error("Invalid quantity for productId={}", item.getProductId());
                    publishInventoryUpdatedEvent(event, item, null, 0, "INVALID_QUANTITY");
                    continue;
                }

                try {
                    Inventory inventory = inventoryService.reduceInventoryAndReturn(
                            item.getProductId(),
                            item.getWarehouseId(),
                            item.getQuantity()
                    );

                    publishInventoryUpdatedEvent(
                            event,
                            item,
                            inventory.getWarehouse(),
                            inventory.getQuantity(),
                            "SUCCESS"
                    );

                } catch (InsufficientStockException e) {

                    log.warn("Insufficient stock for productId={}, warehouseId={}",
                            item.getProductId(), item.getWarehouseId());

                    hasFailures = true;

                    publishInventoryUpdatedEvent(
                            event,
                            item,
                            null,
                            e.getAvailableQuantity(),
                            "INSUFFICIENT_STOCK"
                    );
                }
            }

            // ✅ Update log
            eventLog.setStatus(hasFailures ? "PARTIAL" : "COMPLETED");
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);

            acknowledgment.acknowledge();

        } catch (Exception e) {

            log.error("Kafka processing failed: {}", e.getMessage(), e);

            markEventAsFailed(event.getEventId(), e.getMessage());

            throw new RuntimeException("Kafka processing failed", e);
        }
    }

    private void publishInventoryUpdatedEvent(OrderCreatedEvent orderEvent,
                                              OrderCreatedEvent.OrderItemEvent item,
                                              Warehouse warehouse,
                                              int remainingStock,
                                              String status) {

        InventoryUpdatedEvent event = InventoryUpdatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(orderEvent.getOrderId())
                .productId(item.getProductId())
                .productName(item.getProductName())
                .warehouseId(warehouse != null ? warehouse.getWarehouseId() : null)
                .warehouseName(warehouse != null ? warehouse.getWarehouseName() : null)
                .quantityReduced(item.getQuantity())
                .remainingStock(remainingStock)
                .timestamp(LocalDateTime.now())
                .status(status)
                .build();

       kafkaProducerService.publishInventoryUpdatedEvent(event);
    }

    private boolean isEventAlreadyProcessed(String eventId) {
        return eventLogRepository.existsByEventId(eventId);
    }

    private EventLog createEventLog(BaseEvent event, String status) {
        try {
            EventLog eventLog = new EventLog();
            eventLog.setEventId(event.getEventId());
            eventLog.setEventType(event.getEventType());
            eventLog.setOrderId(event.getOrderId());
            eventLog.setStatus(status);
            eventLog.setEventPayload(objectMapper.writeValueAsString(event));
            eventLog.setCreatedAt(LocalDateTime.now());

            return eventLogRepository.save(eventLog);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create event log", e);
        }
    }

    private void markEventAsFailed(String eventId, String errorMessage) {
        eventLogRepository.findByEventId(eventId).ifPresent(eventLog -> {
            eventLog.setStatus("FAILED");
            eventLog.setErrorMessage(errorMessage);
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);
        });
    }
    private boolean isInvalidOrDuplicate(BaseEvent event, Acknowledgment acknowledgment) {

        if (event.getEventId() == null || event.getEventId().isBlank()
                || event.getOrderId() == null) {

            log.error("Invalid event received");
            acknowledgment.acknowledge();
            return true;
        }

        if (isEventAlreadyProcessed(event.getEventId())) {
            log.warn("Duplicate event: {}", event.getEventId());
            acknowledgment.acknowledge();
            return true;
        }

        return false;
    }

    @KafkaListener(
            topics = "${kafka.topics.inventory-events:inventory-events}",
            groupId = "${kafka.consumer.group-id:order-group}",
            containerFactory = "kafkaListenerContainerFactory",
            properties = {
                    "spring.json.value.default.type=com.oms.event.InventoryUpdatedEvent"
            }
    )
    @Retryable(
            retryFor = {Exception.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, multiplier = 2)
    )
    public void consumeInventoryEvent(@Payload InventoryUpdatedEvent event,
                                      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                      @Header(KafkaHeaders.OFFSET) long offset,
                                      Acknowledgment acknowledgment)
    {
        log.info("Received InventoryUpdatedEvent: eventId={}, orderId={} , status={}",
                event.getEventId(), event.getOrderId(), event.getStatus());
        boolean hasFailures = false;
        EventLog eventLog = createEventLog(event, "PROCESSING");
        try {
            if (isInvalidOrDuplicate(event, acknowledgment)) {
                return;
            }
            if(event.getStatus().equals("SUCCESS")) {
                Integer orderId = Math.toIntExact(event.getOrderId());
                orderService.updateOrderStatus(orderId, OrderStatus.PROCESSED.name());
            }
            else{
                Integer orderId = Math.toIntExact(event.getOrderId());
                orderService.updateOrderStatus(orderId, OrderStatus.FAILED.name());
                hasFailures = true;
            }

            // ✅ Update log
            eventLog.setStatus(hasFailures ? "PARTIAL" : "COMPLETED");
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);

            acknowledgment.acknowledge();
        }catch (Exception e) {

            log.error("Kafka processing failed: {}", e.getMessage(), e);

            markEventAsFailed(event.getEventId(), e.getMessage());

            throw new RuntimeException("Kafka processing failed", e);
        }

    }
}