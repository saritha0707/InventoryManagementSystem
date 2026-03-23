package com.oms.repository;

import com.oms.entity.OrderItem;
import com.oms.entity.Orders;
import com.oms.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderItemRepository extends JpaRepository<OrderItem, Integer> {

    //fetch all items for a given order
    List<OrderItem> findByOrder_OrderId(int orderId);

    //fetch all items for a given product
    List<OrderItem> findByProduct_ProductId(int productId);

    List<OrderItem> findByOrder(Orders order);

    List<OrderItem> findByProduct(Product product);
}
