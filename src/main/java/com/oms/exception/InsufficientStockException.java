package com.oms.exception;

/**
 * Exception thrown when inventory stock is insufficient for order fulfillment.
 * This is a business exception and should NOT be retried.
 */
public class InsufficientStockException extends RuntimeException {

    private final Long productId;
    private final int availableStock;
    private final int requestedQuantity;

    public InsufficientStockException(Integer productId, int availableStock, int requestedQuantity) {
        super(String.format("Insufficient stock: productId=%d, available=%d, requested=%d",
                productId, availableStock, requestedQuantity));
        this.productId = Long.valueOf(productId);
        this.availableStock = availableStock;
        this.requestedQuantity = requestedQuantity;
    }

    public Long getProductId() {
        return productId;
    }

    public int getAvailableStock() {
        return availableStock;
    }

    public int getRequestedQuantity() {
        return requestedQuantity;
    }
}
