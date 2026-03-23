package com.oms.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class ProductRequest {

    @NotBlank(message = "Product Name must not be blank")
    private String productName;

    @NotBlank(message = "Product description must not be blank")
    private String description;

    @NotNull(message = "Product price is mandatory field")
    @DecimalMin(value = "1.0", message = "Product price must be greater than 0")
    private BigDecimal price;

    @NotBlank(message = "Product category must not be blank")
    private String category;
}