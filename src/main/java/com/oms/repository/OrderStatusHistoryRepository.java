package com.oms.repository;

import com.oms.entity.Orders;
import com.oms.entity.OrdersStatusHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface OrderStatusHistoryRepository extends JpaRepository<OrdersStatusHistory, Integer> {
}


