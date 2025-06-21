package org.gojo.learn.scheduler.sdk.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Entity
@Builder
@Table(name = "scheduled_tasks")
@NoArgsConstructor
@AllArgsConstructor
public class Task {

    @Id
    private String id;

    @Column(nullable = false)
    private String type;

    @Column(name = "execute_at", nullable = false)
    private Instant executeAt;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    private TaskStatus status = TaskStatus.SCHEDULED;

}
