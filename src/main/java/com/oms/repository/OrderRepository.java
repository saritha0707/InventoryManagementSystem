package com.oms.repository;

import com.oms.entity.Customer;
import com.oms.entity.Orders;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderRepository extends JpaRepository<Orders, Integer> {

    List<Orders> findByCustomer(Customer customerId);
    //Alternative to above method is
    //List<Orders> findByCustomer_CustomerId(Long customerId);

    List<Orders> findByStatus(String status);

    List<Orders> findByGuestEmail(String guestEmail);
}


