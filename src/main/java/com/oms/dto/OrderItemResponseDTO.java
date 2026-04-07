package com.oms.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderItemResponseDTO {

    private int productId;
    private String productName;
    private int quantity;
    private BigDecimal price;

    //To exclude below fields when the value is null
    // ✅ NEW: Inventory status fields
   // @JsonInclude(JsonInclude.Include.NON_NULL)
   // private String inventoryStatus;   // SUCCESS, INSUFFICIENT_STOCK, PENDING
   // @JsonInclude(JsonInclude.Include.NON_NULL)
   // private Integer availableQuantity;   // Available stock quantity
}