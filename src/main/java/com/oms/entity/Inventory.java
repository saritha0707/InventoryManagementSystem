package com.oms.entity;

import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "inventory",uniqueConstraints =
                @UniqueConstraint(columnNames = {"product_id", "warehouse_id"}))
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int inventoryId;

    @Column(nullable = false)
    private int quantity;

    private LocalDateTime lastUpdated = LocalDateTime.now();

    @ManyToOne
    @JoinColumn(name = "product_id",nullable = false)
    @JsonBackReference
    private Product product;

    @ManyToOne
    @JoinColumn(name = "warehouse_id",nullable = false)
    @JsonBackReference
    private Warehouse warehouse;
}
