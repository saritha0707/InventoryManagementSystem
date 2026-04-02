package com.oms.dto;

import lombok.Data;

@Data
public class OrderStatusUpdateResponseDTO {

    int id;
    String status;
    String message;
}
