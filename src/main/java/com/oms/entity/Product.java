package com.oms.entity;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int productId;

    private String productName;
    @Column(name = "description")
    private String productDescription;
    private BigDecimal price;
    private String category;
    private LocalDateTime createdAt = LocalDateTime.now();

    @OneToMany(mappedBy = "product")
    @JsonManagedReference
    private List<OrderItem> orderItems;

    @OneToMany(mappedBy = "product")
    @JsonManagedReference
    private List<Inventory> inventories;

}
