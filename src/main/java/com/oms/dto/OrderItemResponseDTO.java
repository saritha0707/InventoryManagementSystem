package com.oms.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderItemResponseDTO {

    private int productId;
    private int quantity;
    private BigDecimal price;

}