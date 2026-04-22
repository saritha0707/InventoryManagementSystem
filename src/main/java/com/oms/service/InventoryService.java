package com.oms.service;

import com.oms.dto.InventoryRequestDTO;
import com.oms.dto.InventoryResponseDTO;
import com.oms.entity.Inventory;
import com.oms.entity.Product;
import com.oms.entity.Warehouse;
import com.oms.enums.InventoryStatus;
import com.oms.event.InventoryCheckEvent;
import com.oms.event.InventoryCheckResponseEvent;
import com.oms.event.OrderItemEventResponse;
import com.oms.exception.InsufficientStockException;
import com.oms.exception.InvalidInventoryException;
import com.oms.exception.ResourceNotFoundException;
import com.oms.mapper.InventoryMapper;
import com.oms.repository.InventoryRepository;
import com.oms.repository.ProductRepository;
import com.oms.repository.WarehouseRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final ProductRepository productRepository;
    private final WarehouseRepository warehouseRepository;

    public InventoryService(InventoryRepository inventoryRepository,
                            ProductRepository productRepository,
                            WarehouseRepository warehouseRepository) {
        this.inventoryRepository = inventoryRepository;
        this.productRepository = productRepository;
        this.warehouseRepository = warehouseRepository;
    }

    public Inventory getInventoryOrThrow(int productId, int warehouseId) {

        return inventoryRepository
                .findByProduct_ProductIdAndWarehouse_WarehouseId(productId, warehouseId)
                .orElseThrow(() -> {
                    log.error("Inventory not found for productId={} and warehouseId={}",
                            productId, warehouseId);
                    return new InvalidInventoryException(
                            "Inventory not found for productId=" + productId +
                                    " and warehouseId=" + warehouseId);
                });
    }

    // ✅ 1. Add or Update Inventory
    @Transactional
    public InventoryResponseDTO addOrUpdateInventory(InventoryRequestDTO request) {

        log.info("Add/Update inventory request received: productId={}, warehouseId={}, quantity={}",
                request.getProductId(), request.getWarehouseId(), request.getQuantity());

        if (request.getProductId() <= 0 || request.getWarehouseId() <= 0) {
            throw new InvalidInventoryException("ProductId and WarehouseId must be valid positive numbers");
        }

        if (request.getQuantity() < 0) {
            throw new InvalidInventoryException("Quantity cannot be negative");
        }

        Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> {
                    log.error("Product not found with id={}", request.getProductId());
                    return new ResourceNotFoundException("Product not found: " + request.getProductId());
                });

        Warehouse warehouse = warehouseRepository.findById(request.getWarehouseId())
                .orElseThrow(() -> {
                    log.error("Warehouse not found with id={}", request.getWarehouseId());
                    return new ResourceNotFoundException("Warehouse not found: " + request.getWarehouseId());
                });

        Inventory inventory = inventoryRepository
                .findByProduct_ProductIdAndWarehouse_WarehouseId(
                        request.getProductId(),
                        request.getWarehouseId())
                .orElse(null);

        if (inventory == null) {
            log.info("Creating new inventory record");
            inventory = InventoryMapper.toEntity(request, product, warehouse);
        } else {
            log.info("Updating existing inventory record with id={}", inventory.getInventoryId());
            InventoryMapper.updateEntity(inventory, request);
        }

        Inventory saved = inventoryRepository.save(inventory);

        log.info("Inventory saved successfully with id={}", saved.getInventoryId());

        return InventoryMapper.toResponse(saved);
    }

    // ✅ 2. Get Product Availability
    public List<InventoryResponseDTO> getProductAvailability(int productId) {

        log.info("Fetching inventory for productId={}", productId);

        List<Inventory> inventories = inventoryRepository.findByProduct_ProductId(productId);

        if (inventories.isEmpty()) {
            log.warn("No inventory found for productId={}", productId);
        }

        return InventoryMapper.toResponseList(inventories);
    }

    public boolean isProductAvailable(int productId, int warehouseId, int quantity) {
        Inventory inventory = inventoryRepository.findByProduct_ProductIdAndWarehouse_WarehouseId(productId, warehouseId).orElseThrow(() -> new ResourceNotFoundException("Inventory not found"));
        boolean status = false;
        if (inventory.getQuantity() >= quantity)
            status = true;
        return status;
    }

    //Check availability and return InventoryCheckResponse Object
    public InventoryCheckResponseEvent checkAvailability(InventoryCheckEvent event)
    {
        InventoryCheckResponseEvent responseEvent = new InventoryCheckResponseEvent();
        responseEvent.setEventId(event.getEventId());
        List<OrderItemEventResponse> orderItems = event.getOrderItems().stream()
                .map(itemDTO -> {
                    OrderItemEventResponse itemEventResponse = new OrderItemEventResponse();
                    Inventory inventory = getInventoryOrThrow(itemDTO.getProductId(),itemDTO.getWarehouseId());
                    itemEventResponse.setProductId(inventory.getProduct().getProductId());
                    itemEventResponse.setProductName(inventory.getProduct().getProductName());
                    // Requested quantity
                    itemEventResponse.setQuantity(itemDTO.getQuantity());
                    itemEventResponse.setStatus(isProductAvailable(itemDTO.getProductId(),itemDTO.getWarehouseId(),itemDTO.getQuantity())? InventoryStatus.AVAILABLE.name():InventoryStatus.INSUFFICIENT_STOCK.name());
                    itemEventResponse.setWarehouseId(inventory.getWarehouse().getWarehouseId());
                    itemEventResponse.setWarehouseName(inventory.getWarehouse().getWarehouseName());
                    //Quantity available in DB
                    itemEventResponse.setAvailableCount(inventory.getQuantity());
                    if(itemEventResponse.getStatus().equalsIgnoreCase(InventoryStatus.INSUFFICIENT_STOCK.name())){
                        //quantity which is there in DB
                        itemEventResponse.setAvailableCount(inventory.getQuantity());
                    }
                    itemEventResponse.setPrice(inventory.getProduct().getPrice().intValue());
                  return itemEventResponse;
                }).collect(Collectors.toList());
        responseEvent.setOrderItemCheckResponse(orderItems);
        return responseEvent;
    }

    // ✅ Core logic (single source of truth)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Inventory reduceInventoryAndReturn(int productId, int warehouseId, int quantity) {

        if (quantity <= 0) {
            throw new InvalidInventoryException("Quantity must be greater than 0");
        }

        Inventory inventory = getInventoryOrThrow(productId, warehouseId);

        if (inventory.getQuantity() < quantity) {
            throw new InsufficientStockException(
                    productId,
                    inventory.getQuantity(),   // available
                    quantity                   // requested
            );
        }

        int previous = inventory.getQuantity();

        inventory.setQuantity(previous - quantity);
        inventory.setLastUpdated(LocalDateTime.now());

        inventoryRepository.save(inventory);

        log.info("Inventory reduced: productId={}, previous={}, new={}",
                productId, previous, inventory.getQuantity());

        return inventory;
    }

    // ✅ Wrapper for API
    @Transactional
    public InventoryResponseDTO reduceInventory(int productId, int warehouseId, int quantity) {
        return InventoryMapper.toResponse(
                reduceInventoryAndReturn(productId, warehouseId, quantity)
        );
    }

    // ✅ FIXED restore logic
    @Transactional
    public InventoryResponseDTO restoreInventory(int productId, int warehouseId, int quantity) {

        if (quantity <= 0) {
            throw new InvalidInventoryException("Quantity must be greater than 0");
        }

        Inventory inventory = getInventoryOrThrow(productId, warehouseId);

        inventory.setQuantity(inventory.getQuantity() + quantity);
        inventory.setLastUpdated(LocalDateTime.now());
        log.info("Saved inventory to DB: {}", inventory.getQuantity());
        inventoryRepository.saveAndFlush(inventory);

        log.info("Inventory restored: productId={}, newQty={}",
                productId, inventory.getQuantity());
        return InventoryMapper.toResponse(inventory);
    }

    // ✅ NEW: Safe inventory restoration (doesn't throw exception if inventory doesn't exist)
    @Transactional
    public boolean restoreInventoryIfExists(int productId, int warehouseId, int quantity) {
        try {
            if (quantity <= 0) {
                log.warn("Skipping inventory restoration: invalid quantity={} for productId={}, warehouseId={}",
                        quantity, productId, warehouseId);
                return false;
            }

            // Try to find inventory
            Inventory inventory = inventoryRepository
                    .findByProduct_ProductIdAndWarehouse_WarehouseId(productId, warehouseId)
                    .orElse(null);

            if (inventory == null) {
                // Inventory doesn't exist - log warning but don't fail
                log.warn("Inventory not found for restoration: productId={}, warehouseId={}. " +
                        "This can happen if inventory was never reduced for this item.",
                        productId, warehouseId);
                return false;
            }

            // Restore the inventory
            inventory.setQuantity(inventory.getQuantity() + quantity);
            inventory.setLastUpdated(LocalDateTime.now());
            inventoryRepository.saveAndFlush(inventory);

            log.info("Inventory restored successfully: productId={}, warehouseId={}, newQty={}",
                    productId, warehouseId, inventory.getQuantity());
            return true;

        } catch (Exception e) {
            log.error("Unexpected error during inventory restoration: productId={}, warehouseId={}, error={}",
                    productId, warehouseId, e.getMessage());
            return false;
        }
    }
}