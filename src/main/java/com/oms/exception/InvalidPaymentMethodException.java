package com.oms.exception;

public class InvalidPaymentMethodException extends RuntimeException{

    public InvalidPaymentMethodException(String message)
    {
        super(message);
    }
}
