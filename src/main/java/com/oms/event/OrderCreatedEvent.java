package com.oms.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.util.List;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent extends BaseEvent {

    private Integer customerId;
    private String customerName;
    private String guestName;
    private String guestEmail;
    private String guestPhone;
    private BigDecimal totalAmount;
    private List<OrderItemEvent> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItemEvent {
        private Long orderItemId;
        private Integer productId;
        private String productName;
        private Integer quantity;
        private BigDecimal price;
        private String warehouseName;
        private Integer warehouseId;
    }

    @Override
    public String getEventType() {
        return "ORDER_CREATED";
    }
}