package com.oms.mapper;

import com.oms.dto.InventoryRequest;
import com.oms.dto.InventoryResponse;
import com.oms.entity.Inventory;
import com.oms.entity.Product;
import com.oms.entity.Warehouse;

import java.time.LocalDateTime;

public class InventoryMapper {

    // ✅ Convert Request → Entity
    public static Inventory toEntity(InventoryRequest request, Product product, Warehouse warehouse) {

        Inventory inventory = new Inventory();
        inventory.setProduct(product);
        inventory.setWarehouse(warehouse);
        inventory.setQuantity(request.getQuantity());
        inventory.setLastUpdated(LocalDateTime.now());

        return inventory;
    }

    // ✅ Convert Entity → Response
    public static InventoryResponse toResponse(Inventory inventory) {

        return InventoryResponse.builder()
                .inventoryId((long) inventory.getInventoryId())
                .productId((long) inventory.getProduct().getProductId())
                .productName(inventory.getProduct().getProductName())
                .warehouseId((long) inventory.getWarehouse().getWarehouseId())
                .warehouseName(inventory.getWarehouse().getWarehouseName())
                .quantity(inventory.getQuantity())
                .lastUpdated(inventory.getLastUpdated())
                .build();
    }
}
