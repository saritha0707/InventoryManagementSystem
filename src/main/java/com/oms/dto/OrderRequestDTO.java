package com.oms.dto;

import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;

@Data
public class OrderRequestDTO {

    @NotEmpty(message = "Order must contain at least one item")
    private List<OrderItemRequestDTO> items;
}