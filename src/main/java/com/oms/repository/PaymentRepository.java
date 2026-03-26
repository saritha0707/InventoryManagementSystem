package com.oms.repository;

import com.oms.entity.Orders;
import com.oms.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository

public interface PaymentRepository extends JpaRepository<Payment, Integer> {

    //Fetch Payments for a specific order
    List<Payment> findByOrder_OrderId(int orderId);

    //Fetch by payment status(SUCCESS,FAILED)
    List<Payment> findByPaymentStatus(String paymentStatus);

    //Fetch latest Payment for an order
    List<Payment> findByOrder(Orders order);
}
