package com.oms.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "customers")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Customer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int customerId;

    private String name;
    @Column(unique = true)
    private String email;
    private String phone;
    private String address;
    private LocalDateTime createdAt = LocalDateTime.now();

    @OneToMany(mappedBy = "customer")
    private List<Orders> orders;
}
