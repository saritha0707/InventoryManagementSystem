package com.OrderManagementSystem.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oms.InventoryManagementSystemApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
@ContextConfiguration(classes = InventoryManagementSystemApplication.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OrderService orderService;

    @Autowired
    private ObjectMapper objectMapper;

    private OrderStatusUpdateRequestDTO request;

    private OrderStatusUpdateResponseDTO response;

    @Test
    void testUpdateOrderStatus_success() throws Exception {

        // ✅ Arrange (prepare request)
        request = new OrderStatusUpdateRequestDTO();
        request.setOrderId(1);
        request.setOrder_status("PROCESSED");

        //Mock Response
        response = new OrderStatusUpdateResponseDTO();
        response.setId(1);
        response.setStatus(OrderStatus.PROCESSED.name());
        response.setMessage("Order status updated successfully");

        // ✅ Mock service behavior
        when(orderService.updateOrderStatus(1, OrderStatus.PROCESSED.name()))
                .thenReturn(response);

        // ✅ Act + Assert
        mockMvc.perform(put("/orders/status/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.status").value(OrderStatus.PROCESSED.name()))
                .andExpect(jsonPath("$.message")
                        .value("Order status updated successfully"));
    }

    @Test
    void testUpdateOrderStatus_badRequest_missingOrderStatus() throws Exception {

        // ✅ Arrange (prepare request)
        request = new OrderStatusUpdateRequestDTO();
        request.setOrderId(1);

        // ✅ We dont need to mock service for bad request

        // ✅ Act + Assert
        mockMvc.perform(put("/orders/status/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation Failed"))
                .andExpect(jsonPath("$.message")
                        .value("order_status: order_status is mandatory field"));
    }

    @Test
    void testUpdateOrderStatus_badRequest_missingOrderId() throws Exception {

        // ✅ Arrange (prepare request)
        request = new OrderStatusUpdateRequestDTO();
        //request.setOrderId(1);
        request.setOrder_status(OrderStatus.SHIPPED.name());

        // ✅ We dont need to mock service for bad request

        // ✅ Act + Assert
        mockMvc.perform(put("/orders/status/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation Failed"))
                .andExpect(jsonPath("$.message")
                        .value("orderId: OrderId is mandatory field"));
    }

    @Test
    void testUpdateOrderStatus_badRequest_missingOrderIdOrderStatus() throws Exception {

        // ✅ Arrange (prepare request)
        request = new OrderStatusUpdateRequestDTO();

        // ✅ We dont need to mock service for bad request

        // ✅ Act + Assert
        mockMvc.perform(put("/orders/status/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation Failed"))
                .andExpect(jsonPath("$.message")
                        .value(containsString("order_status: order_status is mandatory field")))
                .andExpect(jsonPath("$.message").value(containsString("orderId: OrderId is mandatory field")));
    }

    @Test
    void testUpdateOrderStatus_badRequest_invalidStatus() throws Exception
    {
        // ✅ Arrange (prepare request)
        request = new OrderStatusUpdateRequestDTO();
        request.setOrderId(1);
        request.setOrder_status("INVALID");

        //mock service for invalid status
        when(orderService.updateOrderStatus(1,"INVALID")).thenThrow(new InvalidOrderStatusException("Not a valid status"));

        // ✅ Act + Assert
        mockMvc.perform(put("/orders/status/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message")
                        .value("Not a valid status"));
    }

}