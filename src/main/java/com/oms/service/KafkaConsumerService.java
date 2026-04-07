package com.oms.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.entity.EventLog;
import com.oms.entity.Inventory;
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
import java.util.ArrayList;
import java.util.List;
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
                            .productName(item.getProductName())
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
                            .productName(item.getProductName())
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
                            .productName(item.getProductName())
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
                            .productName(item.getProductName())
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
            publishConsolidatedInventoryUpdatedEvent(event, itemResults, overallStatus);

            acknowledgment.acknowledge();

        } catch (Exception e) {

            log.error("Kafka processing failed: {}", e.getMessage(), e);

            markEventAsFailed(event.getEventId(), e.getMessage());

            throw new RuntimeException("Kafka processing failed", e);
        }
    }

    /**
     * ✅ NEW: Publish consolidated event with all item results
     */
    private void publishConsolidatedInventoryUpdatedEvent(OrderCreatedEvent orderEvent,
                                                          List<InventoryUpdatedEvent.ItemResult> itemResults,
                                                          String overallStatus) {

        InventoryUpdatedEvent event = InventoryUpdatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(orderEvent.getOrderId())
                .timestamp(LocalDateTime.now())
                .itemResults(itemResults)
                .overallStatus(overallStatus)
                .status(overallStatus)
                .build();

        kafkaProducerService.publishInventoryUpdatedEvent(event);

        log.info("Published consolidated InventoryUpdatedEvent: orderId={}, overallStatus={}, itemCount={}",
                orderEvent.getOrderId(), overallStatus, itemResults.size());
    }

    /**
     * ✅ UPDATED: Consumes InventoryUpdatedEvent with consolidated item results
     * Updates order status to PARTIAL if ANY item has insufficient stock
     */
    @KafkaListener(
            topics = "${kafka.topics.inventory-events:inventory-events}",
            groupId = "${kafka.consumer.group-id:order-group}-inventory",
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
        log.info("Received InventoryUpdatedEvent: eventId={}, orderId={}, overallStatus={}",
                event.getEventId(), event.getOrderId(), event.getOverallStatus());
        
        EventLog eventLog = createEventLog(event, "PROCESSING");
        
        try {
            if (isInvalidOrDuplicate(event, acknowledgment)) {
                return;
            }

            Integer orderId = Math.toIntExact(event.getOrderId());

            // ✅ Determine order status based on overall inventory result
            OrderStatus targetStatus;
            String logStatus;

            if (event.getOverallStatus() != null) {
                // ✅ New consolidated format
                switch (event.getOverallStatus()) {
                    case "SUCCESS":
                        targetStatus = OrderStatus.PROCESSED;
                        logStatus = "COMPLETED";
                        break;
                    case "PARTIAL":
                        targetStatus = OrderStatus.PARTIAL;
                        logStatus = "PARTIAL";
                        break;
                    case "FAILED":
                        targetStatus = OrderStatus.FAILED;
                        logStatus = "FAILED";
                        break;
                    default:
                        targetStatus = OrderStatus.FAILED;
                        logStatus = "FAILED";
                }
            } else if (event.getStatus() != null) {
                // ✅ Fallback for legacy format
                switch (event.getStatus()) {
                    case "SUCCESS":
                        targetStatus = OrderStatus.PROCESSED;
                        logStatus = "COMPLETED";
                        break;
                    default:
                        targetStatus = OrderStatus.FAILED;
                        logStatus = "FAILED";
                }
            } else {
                targetStatus = OrderStatus.FAILED;
                logStatus = "FAILED";
            }

            // ✅ Update order status
            try {
                orderService.updateOrderStatus(orderId, targetStatus.name());
                log.info("Order {} status updated to {}", orderId, targetStatus.name());
            } catch (Exception e) {
                log.warn("Could not update order status to {}: {}", targetStatus.name(), e.getMessage());
                // Don't fail the consumer, just log the warning
            }

            // ✅ NEW: Update order items with inventory results
            try {
                if (event.getItemResults() != null && !event.getItemResults().isEmpty()) {
                    orderService.updateOrderItemsWithInventoryResults(orderId, event.getItemResults());
                    log.info("Updated order items with inventory results for orderId={}", orderId);
                }
            } catch (Exception e) {
                log.warn("Could not update order items with inventory results: {}", e.getMessage());
            }

            // ✅ Update event log
            eventLog.setStatus(logStatus);
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);

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
}


