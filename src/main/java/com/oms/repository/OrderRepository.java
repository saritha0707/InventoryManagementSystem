package com.oms.repository;

import com.oms.entity.Customer;
import com.oms.entity.Orders;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.util.List;



@Repository
public interface OrderRepository extends JpaRepository<Orders, Integer> {

    List<Orders> findByCustomer(Customer customerId);
    //Alternative to above method is
    //List<Orders> findByCustomer_CustomerId(int customerId);

    List<Orders> findByStatus(String status);

    List<Orders> findByGuestEmail(String guestEmail);
}


