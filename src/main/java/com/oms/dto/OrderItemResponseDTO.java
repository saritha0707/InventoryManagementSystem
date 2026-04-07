package com.oms.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderItemResponseDTO {

    private int productId;
    private String productName;
    private int quantity;
    private BigDecimal price;

    // ✅ NEW: Inventory status fields
    private String inventoryStatus;      // SUCCESS, INSUFFICIENT_STOCK, PENDING
    private Integer availableQuantity;   // Available stock quantity
}