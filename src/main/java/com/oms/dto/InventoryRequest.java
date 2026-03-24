package com.oms.dto;

import lombok.Data;

@Data
public class InventoryRequest {
    private Long productId;     // Which product
    private Long warehouseId;   // Which warehouse
    private int quantity;       // Stock to add/update
}
