package com.oms.dto;

import lombok.Data;

@Data
public class OrderStatusUpdateRequestDTO {

    int orderId;
    String status;

}
