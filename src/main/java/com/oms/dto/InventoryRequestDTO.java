package com.oms.dto;

import lombok.Data;

@Data
public class InventoryRequestDTO {
    private int productId;     // Which product
    private int warehouseId;   // Which warehouse
    private int quantity;       // Stock to add/update
    private String warehouseName;
}
