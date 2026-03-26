package com.oms.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderItemRequestDTO {

    @NotNull(message = "Product ID is required")
    @Min(value = 1, message = "Product ID must be valid")
    private Integer productId;

    @Min(value = 1, message = "Quantity must be at least 1")
    private int quantity;

    @NotNull(message = "Warehouse ID is required")
    @Min(value = 1, message = "Warehouse ID must be valid")
    private Integer warehouseId;
}