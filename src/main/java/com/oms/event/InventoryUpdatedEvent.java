package com.oms.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryUpdatedEvent {

    private String eventId;
    private Long orderId;
    private Integer productId;
    private String productName;
    private Integer warehouseId;
    private String warehouseName;
    private Integer quantityReduced;
    private Integer remainingStock;
    private LocalDateTime timestamp;
    private String status;  // SUCCESS, FAILED, INSUFFICIENT_STOCK
}
