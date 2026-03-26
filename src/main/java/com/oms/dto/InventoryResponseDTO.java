package com.oms.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class InventoryResponseDTO {

    private int inventoryId;

    private int productId;
    private String productName;

    private int warehouseId;
    private String warehouseName;

    private int quantity;

    private LocalDateTime lastUpdated;

    private boolean isAvailable;
}