package com.oms.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "order_status_history")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrdersStatusHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id",nullable = false)
    private Orders order;

    private LocalDateTime orderDate = LocalDateTime.now();

    @Column(name = "status", length = 50)
    private String status;  // CREATED, PROCESSED, SHIPPED, CANCELLED, PAID, FAILED

    @Column(name = "changed_at", updatable = false)
    private LocalDateTime changedAt;

    @Column(name = "changed_by", length = 100)
    private String changedBy;

    @Column(name = "remarks", length = 255)
    private String remarks;

    // Automatically set timestamp before insert
    @PrePersist
    public void prePersist() {
        this.changedAt = LocalDateTime.now();
    }
}