package com.oms.exception;

public class CustomerOrGuestValidationException extends RuntimeException{
    public CustomerOrGuestValidationException(String message) {
        super(message);
    }
}
