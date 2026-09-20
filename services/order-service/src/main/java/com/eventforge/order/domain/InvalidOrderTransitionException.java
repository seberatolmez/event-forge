package com.eventforge.order.domain;

public class InvalidOrderTransitionException extends RuntimeException {

    public InvalidOrderTransitionException(String message) {
        super(message);
    }
}
