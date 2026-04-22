package com.oms.event;

import com.oms.enums.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryCheckResponseEvent extends BaseEvent {

    List<OrderItemEventResponse> orderItemCheckResponse;

    @Override
    public String getEventType() {
        return EventType.INVENTORY_CHECK_RESPONSE.name();
    }
}