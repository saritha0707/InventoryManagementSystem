package com.oms.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseEvent {

    protected String eventId;
    protected String status;// AVAILABLE, INSUFFICIENT_STOCK

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    protected LocalDateTime timestamp;

    public abstract String getEventType();
}