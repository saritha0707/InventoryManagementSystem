package com.oms.entity;

import com.oms.entity.Customer;
import com.oms.entity.OrderItem;
import com.oms.entity.Payment;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Orders {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int orderId;

    private LocalDateTime orderDate = LocalDateTime.now();

    @Column(nullable = false)
    private String status; // CREATED, PAID, FAILED

    @Column(nullable = false)
    private BigDecimal totalAmount;

    // Guest fields
    private String guestName;
    private String guestEmail;
    private String guestPhone;

    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> orderItems;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<Payment> payments;
}