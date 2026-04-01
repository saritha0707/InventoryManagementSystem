package com.oms.mapper;

import com.oms.dto.InventoryRequestDTO;
import com.oms.dto.InventoryResponseDTO;
import com.oms.entity.Inventory;
import com.oms.entity.Product;
import com.oms.entity.Warehouse;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;


public class InventoryMapper {

    // Convert Request → Entity
    public static Inventory toEntity(InventoryRequestDTO request, Product product, Warehouse warehouse) {

        Inventory inventory = new Inventory();
        inventory.setProduct(product);
        inventory.setWarehouse(warehouse);
        inventory.setQuantity(request.getQuantity());
        inventory.setLastUpdated(LocalDateTime.now());

        return inventory;
    }
    //   Update existing Inventory (used when record EXISTS)
    public static void updateEntity(Inventory inventory,
                                    InventoryRequestDTO request) {

        inventory.setQuantity(request.getQuantity());
        inventory.setLastUpdated(LocalDateTime.now());
    }
    // Convert Entity → Response DTO
    public static InventoryResponseDTO toResponse(Inventory inventory) {

        Product product = inventory.getProduct();
        Warehouse warehouse = inventory.getWarehouse();

        return InventoryResponseDTO.builder()
                .inventoryId(inventory.getInventoryId())
                .productId(product != null ? product.getProductId() : null)
                .productName(product != null ? product.getProductName() : null)
                .warehouseId(warehouse != null ? warehouse.getWarehouseId() : null)
                .warehouseName(warehouse != null ? warehouse.getWarehouseName() : null)
                .quantity(inventory.getQuantity())
                .lastUpdated(inventory.getLastUpdated())
                .isAvailable(inventory.getQuantity() > 0) // ✅ ADDED THIS LINE
                .build();
    }

    //  Convert List<Entity> → List<Response>
    public static List<InventoryResponseDTO> toResponseList(List<Inventory> inventories) {

        if (inventories == null || inventories.isEmpty()) {
            return List.of();
        }

        return inventories.stream()
                .map(InventoryMapper::toResponse)
                .collect(Collectors.toList());
    }
}

