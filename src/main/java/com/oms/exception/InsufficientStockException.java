package com.oms.exception;

/**
 * Exception thrown when inventory stock is insufficient for order fulfillment.
 * This is a business exception and should NOT be retried.
 */
public class InsufficientStockException extends RuntimeException {

    private final int availableQuantity;

    public InsufficientStockException(int productId, int availableQuantity, int requestedQuantity) {
        super("Insufficient stock for productId=" + productId +
                ", available=" + availableQuantity +
                ", requested=" + requestedQuantity);

        this.availableQuantity = availableQuantity;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }
}