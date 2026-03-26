package com.oms.dto;

import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequestDTO {

    @NotEmpty(message = "Order must contain at least one item")
    private List<OrderItemRequestDTO> items;

    private int customerId;

    private String guestName;

    private String guestEmail;

    private String guestPhone;
}