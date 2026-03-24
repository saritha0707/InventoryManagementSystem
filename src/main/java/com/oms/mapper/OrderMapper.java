package com.oms.mapper;

import com.oms.dto.OrderItemResponseDTO;
import com.oms.dto.OrderResponseDTO;
import com.oms.entity.Orders;

import java.util.List;
import java.util.stream.Collectors;

public class OrderMapper {



    // Mapping Method (Orders → OrderResponseDTO)
    public OrderResponseDTO mapToResponseDTO(Orders order) {

        OrderResponseDTO dto = new OrderResponseDTO();

        dto.setId((int) order.getOrderId());
        dto.setCustomerName(order.getCustomer().getName());
        dto.setStatus(order.getStatus());
        dto.setTotalAmount(order.getTotalAmount());

        List<OrderItemResponseDTO> items = order.getOrderItems().stream().map(item -> {
            OrderItemResponseDTO itemDTO = new OrderItemResponseDTO();
            itemDTO.setProductId((int) item.getProduct().getProductId());
            itemDTO.setQuantity(item.getQuantity());
            itemDTO.setPrice(item.getPrice());
            return itemDTO;
        }).collect(Collectors.toList());

        dto.setItems(items);

        return dto;
    }
}
