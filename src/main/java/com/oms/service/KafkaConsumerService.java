package com.oms.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.entity.EventLog;
import com.oms.entity.Inventory;
import com.oms.entity.Warehouse;
import com.oms.event.InventoryUpdatedEvent;
import com.oms.event.OrderCreatedEvent;
import com.oms.exception.InsufficientStockException;
import com.oms.repository.EventLogRepository;
import com.oms.repository.InventoryRepository;
import com.oms.repository.WarehouseRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private WarehouseRepository warehouseRepository;

    @Autowired
    private EventLogRepository eventLogRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Consumes OrderCreatedEvent from Kafka
     * 1. Validates event (idempotency check)
     * 2. For each item: reduce inventory
     * 3. Publish InventoryUpdatedEvent
     * 4. Handle errors with retry
     */
    @KafkaListener(
            topics = "${kafka.topics.order-events:order-events}",
            groupId = "${kafka.consumer.group-id:order-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
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
        log.info("Received OrderCreatedEvent: eventId={}, orderId={}, partition={}, offset={}",
                event.getEventId(), event.getOrderId(), partition, offset);

        try {
            // STEP 1: Validate critical fields FIRST (before idempotency check)
            if (event.getEventId() == null || event.getEventId().isBlank()) {
                log.error("Invalid event: eventId is null or empty. Discarding message.");
                acknowledgment.acknowledge(); // Acknowledge to prevent retry loop
                return;
            }

            if (event.getOrderId() == null) {
                log.error("Invalid event: orderId is null. EventId={}", event.getEventId());
                acknowledgment.acknowledge(); // Acknowledge to prevent retry loop
                return;
            }

            // Validate items and warehouseName
            if (event.getItems() == null || event.getItems().isEmpty()) {
                log.error("Invalid event: items list is null or empty. EventId={}", event.getEventId());
                acknowledgment.acknowledge(); // Acknowledge to prevent retry loop
                return;
            }

            for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
                if (item.getWarehouseName() == null || item.getWarehouseName().isBlank()) {
                    log.error("Invalid event: warehouseName is null for productId={}. EventId={}",
                            item.getProductId(), event.getEventId());
                    acknowledgment.acknowledge(); // Acknowledge to prevent retry loop
                    return;
                }
                if (item.getProductId() == null) {
                    log.error("Invalid event: productId is null. EventId={}", event.getEventId());
                    acknowledgment.acknowledge(); // Acknowledge to prevent retry loop
                    return;
                }
            }

            // STEP 2: Idempotency Check
            if (isEventAlreadyProcessed(event.getEventId())) {
                log.warn("Event already processed, skipping: eventId={}", event.getEventId());
                acknowledgment.acknowledge();
                return;
            }

            // STEP 2: Log event as PROCESSING
            EventLog eventLog = createEventLog(event, "PROCESSING");

            // STEP 3: Process each order item - reduce inventory
            boolean hasFailures = false;
            for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
                try {
                    processInventoryReduction(event, item);
                } catch (InsufficientStockException e) {
                    // Business exception - do not retry, just log and continue
                    log.warn("Skipping item due to insufficient stock: {}", e.getMessage());
                    hasFailures = true;
                    // Event already published in processInventoryReduction
                }
            }

            // STEP 4: Mark event as COMPLETED or PARTIAL
            eventLog.setStatus(hasFailures ? "PARTIAL" : "COMPLETED");
            eventLog.setProcessedAt(LocalDateTime.now());
            eventLogRepository.save(eventLog);

            log.info("Successfully processed OrderCreatedEvent: eventId={}, orderId={}, status={}",
                    event.getEventId(), event.getOrderId(), eventLog.getStatus());

            // STEP 5: Acknowledge message
            acknowledgment.acknowledge();

        } catch (InsufficientStockException e) {
            // Business exception already handled - acknowledge to prevent retry
            log.warn("Order processed with stock issues: eventId={}", event.getEventId());
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process OrderCreatedEvent: eventId={}, orderId={}, error={}",
                    event.getEventId(), event.getOrderId(), e.getMessage(), e);

            // Log failure
            markEventAsFailed(event.getEventId(), e.getMessage());

            // Rethrow for retry mechanism (only for technical failures)
            throw new RuntimeException("Failed to process order event", e);
        }
    }

    /**
     * Process inventory reduction for a single order item
     */
    private void processInventoryReduction(OrderCreatedEvent orderEvent,
                                           OrderCreatedEvent.OrderItemEvent item) {

        // ✅ STEP 1: Validate input
        if (item.getWarehouseName() == null || item.getWarehouseName().isBlank()) {
            throw new RuntimeException("Invalid warehouseName in event for productId=" + item.getProductId());
        }

        if (item.getProductId() == null) {
            throw new RuntimeException("Invalid productId in event");
        }

        try {
            log.info("Processing inventory reduction: orderId={}, productId={}, warehouse={}, quantity={}",
                    orderEvent.getOrderId(), item.getProductId(),
                    item.getWarehouseName(), item.getQuantity());

            // ✅ STEP 2: Fetch warehouse (safe)
            Warehouse warehouse = warehouseRepository
                    .findFirstByWarehouseName(item.getWarehouseName())   // 🔥 FIX HERE
                    .orElseThrow(() -> new RuntimeException(
                            "Warehouse not found: " + item.getWarehouseName()));

            // ✅ STEP 3: Fetch inventory
            Inventory inventory = inventoryRepository
                    .findByProduct_ProductIdAndWarehouse_WarehouseId(
                            item.getProductId(),
                            warehouse.getWarehouseId())
                    .orElseThrow(() -> new RuntimeException(
                            "Inventory not found for product: " + item.getProductId() +
                                    " in warehouse: " + warehouse.getWarehouseName()));

            // ✅ STEP 4: Check stock
            if (inventory.getQuantity() < item.getQuantity()) {
                log.error("Insufficient stock: productId={}, available={}, requested={}",
                        item.getProductId(), inventory.getQuantity(), item.getQuantity());

                // Publish failure event
                publishInventoryUpdatedEvent(orderEvent, item, warehouse,
                        inventory.getQuantity(), "INSUFFICIENT_STOCK");

                // Throw business exception (will not be retried)
                throw new InsufficientStockException(
                        item.getProductId(),
                        inventory.getQuantity(),
                        item.getQuantity());
            }

            // ✅ STEP 5: Update inventory
            int previousQuantity = inventory.getQuantity();
            inventory.setQuantity(previousQuantity - item.getQuantity());
            inventory.setLastUpdated(LocalDateTime.now());
            inventoryRepository.save(inventory);

            log.info("Inventory reduced successfully: productId={}, previousQty={}, newQty={}",
                    item.getProductId(), previousQuantity, inventory.getQuantity());

            // ✅ STEP 6: Publish success
            publishInventoryUpdatedEvent(orderEvent, item, warehouse,
                    inventory.getQuantity(), "SUCCESS");

        } catch (Exception e) {
            log.error("Failed to reduce inventory: productId={}, warehouse={}, error={}",
                    item.getProductId(), item.getWarehouseName(), e.getMessage(), e);
            throw e;
        }
    }
    /**
     * Publish InventoryUpdatedEvent to Kafka
     */
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
                .warehouseId(warehouse.getWarehouseId())
                .warehouseName(warehouse.getWarehouseName())
                .quantityReduced(item.getQuantity())
                .remainingStock(remainingStock)
                .timestamp(LocalDateTime.now())
                .status(status)
                .build();

        kafkaProducerService.publishInventoryUpdatedEvent(event);
    }

    /**
     * Check if event was already processed (idempotency)
     */
    private boolean isEventAlreadyProcessed(String eventId) {
        return eventLogRepository.existsByEventId(eventId);
    }

    /**
     * Create event log entry
     */
    private EventLog createEventLog(OrderCreatedEvent event, String status) {
        try {
            EventLog eventLog = new EventLog();
            eventLog.setEventId(event.getEventId());
            eventLog.setEventType("ORDER_CREATED");
            eventLog.setOrderId(event.getOrderId());
            eventLog.setStatus(status);
            eventLog.setEventPayload(objectMapper.writeValueAsString(event));
            eventLog.setCreatedAt(LocalDateTime.now());

            return eventLogRepository.save(eventLog);
        } catch (Exception e) {
            log.error("Failed to create event log: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create event log", e);
        }
    }

    /**
     * Mark event as failed
     */
    private void markEventAsFailed(String eventId, String errorMessage) {
        try {
            eventLogRepository.findByEventId(eventId).ifPresent(eventLog -> {
                eventLog.setStatus("FAILED");
                eventLog.setErrorMessage(errorMessage);
                eventLog.setProcessedAt(LocalDateTime.now());
                eventLogRepository.save(eventLog);
            });
        } catch (Exception e) {
            log.error("Failed to mark event as failed: {}", e.getMessage(), e);
        }
    }
}
