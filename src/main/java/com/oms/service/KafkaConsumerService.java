package com.oms.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.entity.EventLog;
import com.oms.entity.Inventory;
import com.oms.enums.InventoryStatus;
import com.oms.event.*;
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
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class KafkaConsumerService {

    private final InventoryService inventoryService;
    private final EventLogRepository eventLogRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(InventoryService inventoryService,
                                EventLogRepository eventLogRepository,
                                KafkaProducerService kafkaProducerService,
                                ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.eventLogRepository = eventLogRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    //Consumes InventoryCheckEvent details and process inventory details and call productAvailablity service call and publish message to another kafka topic
    /**
     * ✅ Consumes InventoryCheckEvent, processes inventory for all items,
     * and publishes product availability message to kafka topic
     */
    @KafkaListener(
            topics = "${kafka.topics.inventory-check-request:inventory-check-request}",
            groupId = "${kafka.consumer.group-id:order-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Retryable(
            retryFor = {Exception.class},
            noRetryFor = {InsufficientStockException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, multiplier = 2)
    )
    public void consumeInventoryCreatedEvent(
            @Payload InventoryCheckEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {

        log.info("Received Inventory Check for ProductId: eventId={}",
                event.getEventId());

        try {
            if(event.getOrderItems() == null || event.getOrderItems().isEmpty()) {
                log.error("Invalid event received - no items");
                acknowledgment.acknowledge();
                return;
            }
            
            if (isInvalidOrDuplicate(event, acknowledgment)) {
                return;
            }

            // ✅ Create event log
            EventLog eventLog = createEventLog(event, "PROCESSING");

            // ✅ Track per-item results
            List<InventoryEvent.ItemResult> itemResults = new ArrayList<>();
            boolean hasFailures = false;
            boolean hasSuccesses = true;
           //call this method checkAvailability from service and update eventlog with success status
           // ✅ Process items
           InventoryCheckResponseEvent responseEvent = inventoryService.checkAvailability(event);
           hasFailures = responseEvent.getOrderItemCheckResponse().stream().anyMatch(item -> item.getStatus().equalsIgnoreCase(InventoryStatus.INSUFFICIENT_STOCK.name()));
           hasSuccesses = !hasFailures;
           //publish this response in topic
            kafkaProducerService.publishInventoryCheckResponse(responseEvent);
            // ✅ Determine overall status based on results
            String overallStatus = "SUCCESS";
            if (hasFailures && hasSuccesses) {
                overallStatus = "PARTIAL";
            } else if (hasFailures && !hasSuccesses) {
                overallStatus = "FAILED";
            }

            // ✅ Update event log with overall status
            eventLog.setStatus(overallStatus);
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);

            acknowledgment.acknowledge();

        } catch (Exception e) {

            log.error("Kafka processing failed: {}", e.getMessage(), e);

            markEventAsFailed(event.getEventId(), e.getMessage());

            throw new RuntimeException("Kafka processing failed", e);
        }
    }


    /**
     * ✅ UPDATED: Consumes OrderCreatedEvent, processes inventory for all items,
     * tracks per-item success/failure, and publishes consolidated InventoryUpdatedEvent
     */
    @KafkaListener(
            topics = "${kafka.topics.order-events:order-events}",
            groupId = "${kafka.consumer.group-id:order-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Retryable(
            retryFor = {Exception.class},
            noRetryFor = {InsufficientStockException.class},
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
                log.error("Invalid event received - no items");
                acknowledgment.acknowledge();
                return;
            }

            if (isInvalidOrDuplicate(event, acknowledgment)) {
                return;
            }

            // ✅ Create event log
            EventLog eventLog = createEventLog(event, "PROCESSING");

            // ✅ Track per-item results
            List<InventoryUpdatedEvent.ItemResult> itemResults = new ArrayList<>();
            boolean hasFailures = false;
            boolean hasSuccesses = false;

            // ✅ Process items
            for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {

                if (item.getProductId() == null || item.getWarehouseId() == null) {
                    log.error("Invalid item: productId or warehouseId is null");
                    itemResults.add(InventoryUpdatedEvent.ItemResult.builder()
                            .productId(item.getProductId())
                            .warehouseId(item.getWarehouseId())
                            .requestedQuantity(item.getQuantity())
                            .availableQuantity(0)
                            .status("INVALID_INPUT")
                            .build());
                    hasFailures = true;
                    continue;
                }

                if (item.getQuantity() <= 0) {
                    log.error("Invalid quantity for productId={}", item.getProductId());
                    itemResults.add(InventoryUpdatedEvent.ItemResult.builder()
                            .productId(item.getProductId())
                            .warehouseId(item.getWarehouseId())
                            .requestedQuantity(item.getQuantity())
                            .availableQuantity(0)
                            .status("INVALID_QUANTITY")
                            .build());
                    hasFailures = true;
                    continue;
                }

                try {
                    // ✅ Try to reduce inventory
                    Inventory inventory = inventoryService.reduceInventoryAndReturn(
                            item.getProductId(),
                            item.getWarehouseId(),
                            item.getQuantity()
                    );

                    // ✅ Add success result
                    itemResults.add(InventoryUpdatedEvent.ItemResult.builder()
                            .productId(item.getProductId())
                            .warehouseId(inventory.getWarehouse().getWarehouseId())
                            .warehouseName(inventory.getWarehouse().getWarehouseName())
                            .requestedQuantity(item.getQuantity())
                            .availableQuantity(inventory.getQuantity())
                            .status("SUCCESS")
                            .build());

                    hasSuccesses = true;
                    log.info("Successfully reduced inventory for productId={}", item.getProductId());

                } catch (InsufficientStockException e) {

                    log.warn("Insufficient stock for productId={}, warehouseId={}, requested={}, available={}",
                            item.getProductId(), item.getWarehouseId(), item.getQuantity(), e.getAvailableQuantity());

                    hasFailures = true;

                    // ✅ Add insufficient stock result
                    itemResults.add(InventoryUpdatedEvent.ItemResult.builder()
                            .productId(item.getProductId())
                            .warehouseId(item.getWarehouseId())
                            .requestedQuantity(item.getQuantity())
                            .availableQuantity(e.getAvailableQuantity())
                            .status("INSUFFICIENT_STOCK")
                            .build());
                }
            }

            // ✅ Determine overall status based on results
            String overallStatus = "SUCCESS";
            if (hasFailures && hasSuccesses) {
                overallStatus = "PARTIAL";
            } else if (hasFailures && !hasSuccesses) {
                overallStatus = "FAILED";
            }

            // ✅ Update event log with overall status
            eventLog.setStatus(overallStatus);
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);

            log.info("Order {} inventory processing: overallStatus={}, successes={}, failures={}",
                    event.getOrderId(), overallStatus, hasSuccesses, hasFailures);

            // ✅ Publish consolidated inventory updated event
         //   publishConsolidatedInventoryUpdatedEvent(event, itemResults, overallStatus);

            acknowledgment.acknowledge();

        } catch (Exception e) {

            log.error("Kafka processing failed: {}", e.getMessage(), e);

            markEventAsFailed(event.getEventId(), e.getMessage());

            throw new RuntimeException("Kafka processing failed", e);
        }
    }

    // ✅ Helper methods

    private boolean isEventAlreadyProcessed(String eventId) {
        return eventLogRepository.existsByEventId(eventId);
    }

    private EventLog createEventLog(BaseEvent event, String status) {
        try {
            EventLog eventLog = new EventLog();
            eventLog.setEventId(event.getEventId());
            eventLog.setEventType(event.getEventType());
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

        if (event.getEventId() == null || event.getEventId().isBlank()) {

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
}


