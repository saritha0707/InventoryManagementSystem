package com.oms.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "event_logs", indexes = {
    @Index(name = "idx_event_id", columnList = "eventId", unique = true)
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EventLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(nullable = false)
    private String eventType;  // ORDER_CREATED, INVENTORY_UPDATED

    private Long orderId;

    @Column(nullable = false)
    private String status;  // PROCESSING, COMPLETED, FAILED

    @Column(columnDefinition = "TEXT")
    private String eventPayload;

    private String errorMessage;

    private LocalDateTime createdAt;

    private LocalDateTime processedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
