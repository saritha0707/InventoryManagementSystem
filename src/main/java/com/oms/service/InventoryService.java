package com.oms.service;

import com.oms.dto.InventoryRequest;
import com.oms.dto.InventoryResponse;
import com.oms.entity.Inventory;
import com.oms.entity.Product;
import com.oms.entity.Warehouse;
import com.oms.repository.InventoryRepository;
import com.oms.repository.ProductRepository;
import com.oms.repository.WarehouseRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;


@Service
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

    // ✅ 1. Add or Update Inventory
    @Transactional
    public InventoryResponse addOrUpdateInventory(InventoryRequest request) {

        Product product = productRepository.findById(Math.toIntExact(request.getProductId()))
                .orElseThrow(() -> new RuntimeException("Product not found"));

        Warehouse warehouse = warehouseRepository.findById(Math.toIntExact(request.getWarehouseId()))
                .orElseThrow(() -> new RuntimeException("Warehouse not found"));

        // Check if inventory already exists
        Inventory inventory = inventoryRepository
                .findByProduct_ProductIdAndWarehouse_WarehouseId(
                        Math.toIntExact(request.getProductId()),
                        Math.toIntExact(request.getWarehouseId())
                )
                .orElse(new Inventory());

        inventory.setProduct(product);
        inventory.setWarehouse(warehouse);
        inventory.setQuantity(request.getQuantity());
        inventory.setLastUpdated(LocalDateTime.now());

        Inventory saved = inventoryRepository.save(inventory);

        return mapToResponse(saved);
    }

    // ✅ 2. Get Product Availability
    public List<InventoryResponse> getProductAvailability(int productId) {
        List<Inventory> inventories = inventoryRepository.findByProduct_ProductId(productId);
        return inventories.stream()
                .map(this::mapToResponse)
                .toList();
    }

    // ✅ 3. Reduce Inventory (after order)
    @Transactional
    public void reduceInventory(int productId, int warehouseId, int quantity) {

        Inventory inventory = inventoryRepository
                .findByProduct_ProductIdAndWarehouse_WarehouseId(Math.toIntExact(productId), Math.toIntExact(warehouseId))
                .orElseThrow(() -> new RuntimeException("Inventory not found"));

        if (quantity <= 0) {
            throw new RuntimeException("Quantity must be greater than 0");
        }

        if (inventory.getQuantity() < quantity) {
            throw new RuntimeException("Insufficient stock");
        }

        inventory.setQuantity(inventory.getQuantity() - quantity);
        inventory.setLastUpdated(LocalDateTime.now());

        inventoryRepository.save(inventory);
    }

    // ✅ Mapper Method (Entity → DTO)
    private InventoryResponse mapToResponse(Inventory inventory) {
        return InventoryResponse.builder()
                .inventoryId((int) inventory.getInventoryId())
                .productId((int) inventory.getProduct().getProductId())
                .productName(inventory.getProduct().getProductName())
                .warehouseId((int) inventory.getWarehouse().getWarehouseId())
                .warehouseName(inventory.getWarehouse().getWarehouseName())
                .quantity(inventory.getQuantity())
                .lastUpdated(inventory.getLastUpdated())
                .build();
    }
}