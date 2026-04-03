package com.oms.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryUpdatedEvent extends BaseEvent {

    private Integer productId;
    private String productName;
    private Integer warehouseId;
    private String warehouseName;
    private Integer quantityReduced;
    private Integer remainingStock;

    @Override
    public String getEventType() {
        return "INVENTORY_UPDATED";
    }
}
