package com.oms.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    private String eventId;          // Unique ID for idempotency
    private Long orderId;
    private Integer customerId;
    private String customerName;
    private String guestName;
    private String guestEmail;
    private String guestPhone;
    private BigDecimal totalAmount;
    private String status;
    private LocalDateTime timestamp;
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
}
