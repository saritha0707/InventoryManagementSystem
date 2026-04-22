package com.oms.event;

import com.oms.enums.InventoryStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryEvent extends BaseEvent {

    private Integer productId;
    private Integer warehouseId;

    // ✅ NEW: Consolidated item results (array of per-item inventory statuses)
    private List<ItemResult> itemResults;

    // ✅ NEW: Overall inventory processing result (SUCCESS, PARTIAL, FAILED)
    private String overallStatus;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ItemResult {
        private Integer productId;
        private String productName;
        private Integer warehouseId;
        private String warehouseName;
        private Integer requestedQuantity;
        private Integer availableQuantity;
        private String status; // SUCCESS, INSUFFICIENT_STOCK
    }

    @Override
    public String getEventType() {
        return InventoryStatus.INVENTORY_UPDATED.name();
    }
}


