package com.oms.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
public class OrderResponseDTO {

    private Long id;
    private String status;
    private BigDecimal totalAmount;
    private List<OrderItemResponseDTO> items;
}