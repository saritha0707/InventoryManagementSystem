package com.OrderManagementSystem.modal;

import com.OrderManagementSystem.entity.OrderDetails;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderDAO extends JpaRepository<OrderDetails, Long> {
}
