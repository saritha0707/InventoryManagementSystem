package com.oms.exception;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    //  Handle Resource Not Found
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleResourceNotFound(ResourceNotFoundException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Not Found",
                ex.getMessage()
        );

        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }

    //  Handle Generic Exceptions
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Internal Server Error",
                ex.getMessage()
        );

        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotAllowed(
            HttpRequestMethodNotSupportedException ex){
        String message = "Method " + ex.getMethod() + " is not allowed for this endpoint";

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Method Not Allowed",
                message
        );
        return new ResponseEntity<>(error, HttpStatus.METHOD_NOT_ALLOWED);
    }
    @ExceptionHandler(org.springframework.web.servlet.NoHandlerFoundException.class)
    public ResponseEntity<?> handleNotFound(NoHandlerFoundException ex) {
        String message = "API end point not found";
        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Not Found",
                message

        );
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }
    //  Handle Validation Errors (@Valid DTO)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(MethodArgumentNotValidException ex) {

        String message = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(err -> err.getField() + ": " + err.getDefaultMessage())
                .collect(Collectors.joining(", "));

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Validation Failed",
                message
        );

        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(HttpMessageNotReadableException ex) {

        String message = "Invalid request body";

        // ✅ Case 1: Missing request body
        if (ex.getMessage() != null && ex.getMessage().contains("Required request body is missing")) {
            message = "Request body is missing";
        }

        // ✅ Case 2: Invalid enum / wrong datatype
        else if (ex.getCause() instanceof com.fasterxml.jackson.databind.exc.InvalidFormatException ife) {

            String fieldName = ife.getPath().stream()
                    .map(ref -> ref.getFieldName())
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse("unknown");

            Class<?> targetType = ife.getTargetType();

            if(fieldName.equals("paymentMethod"))
            {
                message = "paymentMethod accepts only CASH_ON_DELIVERY or ONLINE";
            }
            // 🔹 Enum case
            else if (targetType.isEnum()) {
                message = "Invalid value '" + ife.getValue() +
                        "' for field '" + fieldName +
                        "'. Allowed values: " +
                        Arrays.toString(targetType.getEnumConstants());
            }
            // 🔹 Other datatype mismatch
            else {
                message = "Invalid value '" + ife.getValue() +
                        "' for field '" + fieldName + "'";
            }
        }

        // ✅ Case 3: Unknown field
        else if (ex.getCause() instanceof com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException upe) {
            message = "Invalid field: " + upe.getPropertyName();
        }

        // ✅ Case 4: Malformed JSON (fallback)
        else {
            message = "Malformed JSON request";
        }

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Bad Request",
                message
        );

        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    //  Handle Invalid Order State
    @ExceptionHandler(InvalidOrderStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOrderState(InvalidOrderStateException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Bad Request",
                ex.getMessage()
        );

        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(CustomerOrGuestValidationException.class)
    public ResponseEntity<ErrorResponse> handleCustomValidation(CustomerOrGuestValidationException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Validation Failed",
                ex.getMessage()
        );

        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(InvalidOrderStatusException.class)
    public ResponseEntity<ErrorResponse> handleInvalidStatus(InvalidOrderStatusException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Bad Request",
                ex.getMessage()
        );

        return new ResponseEntity<>(error,HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(InvalidPaymentMethodException.class)
    public ResponseEntity<ErrorResponse> handleInvalidStatus(InvalidPaymentMethodException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Bad Request",
                ex.getMessage()
        );

        return new ResponseEntity<>(error,HttpStatus.BAD_REQUEST);
    }
    @ExceptionHandler(InsufficientStockException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientStock(InsufficientStockException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Bad Request",
                ex.getMessage()
        );

        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
    @ExceptionHandler(InvalidInventoryException.class)
    public ResponseEntity<ErrorResponse> handleInvalidQuantity(InvalidInventoryException ex) {

        ErrorResponse error = new ErrorResponse(
                LocalDateTime.now(),
                "Bad Request",
                ex.getMessage()
        );

        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
}
