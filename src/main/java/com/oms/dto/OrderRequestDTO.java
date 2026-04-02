package com.oms.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import com.oms.enums.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequestDTO {

    @NotEmpty(message = "Order must contain at least one item")
    @Valid
    private List<OrderItemRequestDTO> items;

    private Integer customerId; // can be null

    private String guestName;

    @Email(message = "Invalid email format")
    private String guestEmail;

    private String guestPhone;

    //Payment method - ONLINE,CASH_ON_DELIVERY(COD)
    @NotNull(message = "Payment method is mandatory")
    private PaymentMethod paymentMethod;
}