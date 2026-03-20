package com.oms.repository;

import com.oms.entity.Orders;
import com.oms.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    //Fetch Payments for a specific order
    List<Payment> findByOrder_OrderId(Long orderId);

    //Fetch by payment status(SUCCESS,FAILED)
    List<Payment> findByPaymentStatus(String paymentStatus);

    //Fetch latest Payment for an order
    List<Payment> findByOrder(Orders order);
}
