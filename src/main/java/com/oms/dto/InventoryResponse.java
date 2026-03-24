package com.oms.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class InventoryResponse {

    private Long inventoryId;

    private Long productId;
    private String productName;

    private Long warehouseId;
    private String warehouseName;

    private int quantity;

    private LocalDateTime lastUpdated;
}