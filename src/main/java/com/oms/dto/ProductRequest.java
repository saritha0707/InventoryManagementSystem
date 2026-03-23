package com.oms.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class ProductRequest {

    private String productName;
    private String description;
    private BigDecimal price;
    private String category;
}