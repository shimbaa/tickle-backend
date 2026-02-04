package com.profect.tickle.domain.performance.entity;

import com.profect.tickle.domain.member.entity.Member;
import com.profect.tickle.domain.performance.dto.request.PerformanceRequestDto;
import com.profect.tickle.domain.performance.dto.request.UpdatePerformanceRequestDto;
import com.profect.tickle.global.status.Status;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Getter
@Entity
@Table(name = "performance")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class Performance {

    @Id
    @Column(name = "performance_id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 연관 관계
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "member_id", nullable = false)
    private Member member;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "hall_id", nullable = false)
    private Hall hall;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "genre_id", nullable = false)
    private Genre genre;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "status_id", nullable = false)
    private Status status;

    @Column(name = "performance_title", length = 255, nullable = false)
    private String title;

    @Column(name = "performance_price", nullable = false)
    private String price;

    @Column(name = "performance_date", nullable = false)
    private Instant date;

    @Column(name = "performance_runtime", nullable = false)
    private Short runtime;

    @Column(name = "performance_img", length = 255, nullable = false)
    private String img;

    @Column(name = "performance_start_date", nullable = false)
    private Instant startDate;

    @Column(name = "performance_end_date", nullable = false)
    private Instant endDate;

    @Column(name = "performance_is_event", nullable = false)
    private Boolean isEvent;

    @Column(name = "performance_look_count", nullable = false)
    private Short lookCount;

    @Column(name = "performance_created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "performance_updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "performance_deleted_at")
    private Instant deletedAt;

    private Performance(
            String title,
            Member member,
            Genre genre,
            Hall hall,
            Status status,
            Instant date,
            Short runtime,
            String price,
            String img,
            Instant startDate,
            Instant endDate,
            boolean isEvent
    ) {
        this.title = title;
        this.member = member;
        this.genre = genre;
        this.hall = hall;
        this.status = status;
        this.date = date;
        this.runtime = runtime;
        this.price = price;
        this.img = img;
        this.startDate = startDate;
        this.endDate = endDate;
        this.isEvent = isEvent;
        this.lookCount = 0;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }


    public static Performance create(
            PerformanceRequestDto dto,
            Member member,
            Genre genre,
            Hall hall,
            Status status,
            String priceRange
    ) {
        return new Performance(
                dto.getTitle(),
                member,
                genre,
                hall,
                status,
                dto.getDate(),
                dto.getRuntime(),
                priceRange,
                dto.getImg(),
                dto.getStartDate(),
                dto.getEndDate(),
                Boolean.TRUE.equals(dto.getIsEvent())
        );
    }

    public void updateFrom(UpdatePerformanceRequestDto dto) {
        if (dto.getTitle() != null) this.title = dto.getTitle();
        if (dto.getDate() != null) this.date = dto.getDate();
        if (dto.getRuntime() != null) this.runtime = dto.getRuntime();
        if (dto.getImg() != null) this.img = dto.getImg();
        if (dto.getIsEvent() != null) this.isEvent = dto.getIsEvent();
        this.updatedAt = Instant.now();
    }

    public void markAsDeleted() {
        this.deletedAt = Instant.now();
    }

    public boolean isReservationPeriod() {
        return this.startDate.isBefore(Instant.now()) && this.endDate.isAfter(Instant.now());
    }
}
