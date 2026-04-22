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
public class InventoryCheckEvent extends BaseEvent {

    List<OrderItemEvent> orderItems;

    @Override
    public String getEventType() {
        return EventType.INVENTORY_CHECK_EVENT.name();
    }
}