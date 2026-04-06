package com.oms.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class OrderStatusUpdateRequestDTO {

    @NotNull(message = "OrderId is mandatory field")
    private Integer orderId;
    @NotBlank(message = "order_status is mandatory field")
     private String order_status;

}
