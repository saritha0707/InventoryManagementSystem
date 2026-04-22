package com.oms.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderItemEventResponse {

     Integer productId;
     String productName;
     Integer warehouseId;
     String warehouseName;
     Integer price;
     Integer quantity;
     Integer availableCount;
     String status; // AVAILABLE , INSUFFICIENT_STOCK
}