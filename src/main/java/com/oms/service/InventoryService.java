package com.oms.service;

import com.oms.dto.InventoryRequestDTO;
import com.oms.dto.InventoryResponseDTO;
import com.oms.entity.Inventory;
import com.oms.entity.Product;
import com.oms.entity.Warehouse;
import com.oms.exception.ResourceNotFoundException;
import com.oms.mapper.InventoryMapper;
import com.oms.repository.InventoryRepository;
import com.oms.repository.ProductRepository;
import com.oms.repository.WarehouseRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

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

    // ✅ Common Method (Avoid duplication)
    private Inventory getInventoryOrThrow(int productId, int warehouseId) {

        return inventoryRepository
                .findByProduct_ProductIdAndWarehouse_WarehouseId(productId, warehouseId)
                .orElseThrow(() -> {
                    log.error("Inventory not found for productId={} and warehouseId={}",
                            productId, warehouseId);
                    return new ResourceNotFoundException(
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
            throw new IllegalArgumentException("ProductId and WarehouseId must be valid positive numbers");
        }

        if (request.getQuantity() < 0) {
            throw new IllegalArgumentException("Quantity cannot be negative");
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

        List<InventoryResponseDTO> inventories = getProductAvailability(productId);

        return inventories.stream()
                .anyMatch(inv ->
                        inv.getWarehouseId() == warehouseId &&
                                inv.getQuantity() >= quantity
                );
    }

    // ✅ 3. Reduce Inventory (after order)
    @Transactional
    public InventoryResponseDTO reduceInventory(int productId, int warehouseId, int quantity) {

        log.info("Reducing inventory: productId={}, warehouseId={}, quantity={}",
                productId, warehouseId, quantity);

        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }

        Inventory inventory = getInventoryOrThrow(productId, warehouseId);

        if (inventory.getQuantity() < quantity) {
            log.error("Insufficient stock: available={}, requested={}",
                    inventory.getQuantity(), quantity);
            throw new IllegalStateException("Insufficient stock");
        }

        // Reduce stock
        inventory.setQuantity(inventory.getQuantity() - quantity);
        inventory.setLastUpdated(LocalDateTime.now());

        inventoryRepository.save(inventory);

        log.info("Inventory reduced successfully. Remaining quantity={}", inventory.getQuantity());

        return InventoryMapper.toResponseList(List.of(inventory)).get(0);
    }

    // ✅ 4. Restore Inventory (when order is cancelled)
    @Transactional
    public InventoryResponseDTO restoreInventory(int productId, int warehouseId, int quantity) {

        log.info("Restoring inventory: productId={}, warehouseId={}, quantity={}",
                productId, warehouseId, quantity);

        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be greater than 0");
        }

        Inventory inventory = getInventoryOrThrow(productId, warehouseId);

        // Add stock back
        inventory.setQuantity(inventory.getQuantity() + quantity);
        inventory.setLastUpdated(LocalDateTime.now());

        inventoryRepository.save(inventory);

        log.info("Inventory restored successfully. New quantity={}", inventory.getQuantity());

        return InventoryMapper.toResponseList(List.of(inventory)).get(0);
    }
}