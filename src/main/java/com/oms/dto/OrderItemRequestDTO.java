package com.oms.dto;

import com.oms.entity.Customer;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class OrderItemRequestDTO {

    @NotNull(message = "Product ID is required")
    private int productId;

    @Min(value = 1, message = "Quantity must be at least 1")
    private int quantity;

    @NotBlank (message = "Warehouse name is required")
    private String warehousename;

}