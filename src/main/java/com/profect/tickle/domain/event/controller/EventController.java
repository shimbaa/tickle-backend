package com.profect.tickle.domain.event.controller;

import com.profect.tickle.domain.event.dto.response.*;
import com.profect.tickle.domain.event.dto.request.CouponCreateRequestDto;
import com.profect.tickle.domain.event.dto.request.TicketEventCreateRequestDto;
import com.profect.tickle.domain.event.entity.EventType;
import com.profect.tickle.domain.event.service.event.CouponService;
import com.profect.tickle.domain.event.service.event.EventService;
import com.profect.tickle.global.paging.PagingResponse;
import com.profect.tickle.global.response.ResultCode;
import com.profect.tickle.global.response.ResultResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@Profile("redis")
@RestController
@RequestMapping("/api/v1/event")
@RequiredArgsConstructor
@Tag(name = "이벤트", description = "쿠폰 및 티켓 이벤트 관련 API입니다.")
public class EventController {

    private final EventService eventService;
    private final CouponService couponService;

    @PreAuthorize("hasAuthority('ADMIN')")
    @Operation(summary = "쿠폰 이벤트 생성", description = "관리자가 쿠폰 이벤트를 생성합니다.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "쿠폰 생성 요청 DTO",
                    required = true,
                    content = @Content(schema = @Schema(implementation = CouponCreateRequestDto.class))),
            responses = {
                    @ApiResponse(responseCode = "200", description = "이벤트 생성 성공",
                            content = @Content(schema = @Schema(implementation = CouponResponseDto.class))),
                    @ApiResponse(responseCode = "400", description = "중복된 쿠폰 이름 등 유효성 예외")})

    @PostMapping("/coupon")
    public ResultResponse<CouponResponseDto> createCoupon(@Valid @RequestBody CouponCreateRequestDto request) {
        CouponResponseDto response = eventService.createCouponEvent(request);
        return ResultResponse.of(ResultCode.EVENT_CREATE_SUCCESS, response);
    }

    @PreAuthorize("hasRole('HOST')")
    @Operation(summary = "티켓 이벤트 직접 생성", description = "주최자가 티켓 이벤트를 생성합니다.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "티켓 이벤트 생성 요청 DTO",
                    required = true,
                    content = @Content(schema = @Schema(implementation = TicketEventCreateRequestDto.class))),
            responses = {
                    @ApiResponse(responseCode = "200", description = "이벤트 생성 성공",
                            content = @Content(schema = @Schema(implementation = TicketEventResponseDto.class))),
                    @ApiResponse(responseCode = "404", description = "존재하지 않는 좌석 또는 상태 ID")})

    @PostMapping("/ticket")
    public ResultResponse<TicketEventResponseDto> createTicketEvent(@Valid @RequestBody TicketEventCreateRequestDto request) {
        TicketEventResponseDto response = eventService.createTicketEvent(request);
        return ResultResponse.of(ResultCode.EVENT_CREATE_SUCCESS, response);
    }

    @Operation(summary = "티켓 이벤트 응모", description = "사용자가 티켓 이벤트에 응모합니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "응모 성공",
                            content = @Content(schema = @Schema(implementation = TicketApplyResponseDto.class))),
                    @ApiResponse(responseCode = "400", description = "포인트 부족, 중복 응모 등 예외 발생")})
    @PostMapping("/ticket/{eventId}")
    public ResultResponse<String> applyTicketEvent(@PathVariable Long eventId) {
        eventService.applyTicketEvent(eventId);
        return ResultResponse.of(ResultCode.EVENT_APPLY_SUCCESS, "당첨 됐게 안됐게");
    }

    @Operation(summary = "쿠폰 이벤트 응모", description = "유저가 쿠폰 이벤트에 응모하여 쿠폰을 발급받습니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "쿠폰 발급 성공",
                            content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = "쿠폰 소진, 중복 발급 등 예외")})
    @PostMapping("/coupon/{eventId}")
    public ResultResponse<String> issueCoupon(@PathVariable Long eventId) {
        eventService.issueCoupon(eventId);
        return ResultResponse.of(ResultCode.COUPON_ISSUE_SUCCESS, "[이벤트 쿠폰 지급 완료]: eventId = " + eventId);
    }

    @Operation(summary = "쿠폰 이벤트 상세 조회", description = "특정 쿠폰 ID에 해당하는 쿠폰 이벤트 정보를 반환합니다.")
    @GetMapping("/coupon/{couponId}")
    public ResultResponse<CouponListResponseDto> getSpecialCouponDetail(@PathVariable Long couponId) {
        CouponListResponseDto dto = couponService.getSpecialCouponDetailById(couponId);
        return ResultResponse.of(ResultCode.EVENT_INFO_SUCCESS, dto);
    }

    @Operation(summary = "이벤트 목록 조회", description = "쿠폰 또는 티켓 이벤트를 페이징으로 조회합니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "조회 성공",
                            content = @Content(schema = @Schema(implementation = PagingResponse.class))),
                    @ApiResponse(responseCode = "400", description = "유효하지 않은 이벤트 타입")})
    @GetMapping
    public ResultResponse<PagingResponse<EventListResponseDto>> getEventList(@RequestParam("type") EventType eventType,
                                                                             @RequestParam("page") int page,
                                                                             @RequestParam("size") int size) {

        PagingResponse<EventListResponseDto> response = eventService.getEventList(eventType, page, size);
        return ResultResponse.of(ResultCode.EVENT_INFO_SUCCESS, response);
    }

    @Operation(summary = "티켓 이벤트 상세 조회", description = "티켓 이벤트의 상세 정보를 조회합니다.",
            responses = {@ApiResponse(responseCode = "200", description = "조회 성공",
                    content = @Content(schema = @Schema(implementation = TicketEventDetailResponseDto.class)))})
    @GetMapping("/ticket/{eventId}")
    public ResultResponse<TicketEventDetailResponseDto> getTicketEventDetail(@PathVariable Long eventId) {
        TicketEventDetailResponseDto detail = eventService.getTicketEventDetail(eventId);
        return ResultResponse.of(ResultCode.EVENT_INFO_SUCCESS, detail);
    }

    @Operation(summary = "티켓 이벤트 키워드 검색", description = "키워드로 티켓 이벤트를 검색합니다.",
            responses = {@ApiResponse(responseCode = "200", description = "검색 성공",
                    content = @Content(schema = @Schema(implementation = PagingResponse.class)))})
    @GetMapping("/ticket/search")
    public ResultResponse<PagingResponse<TicketListResponseDto>> searchTicketEvents(@RequestParam String keyword,
                                                                                    @RequestParam int page,
                                                                                    @RequestParam int size) {
        PagingResponse<TicketListResponseDto> response = eventService.searchTicketEvents(keyword, page, size);
        return ResultResponse.of(ResultCode.EVENT_INFO_SUCCESS, response);
    }

    @Operation(summary = "랜덤 이벤트 5개 조회", description = "현재 진행 중인 이벤트 중 5개를 랜덤으로 조회합니다.",
            responses = {@ApiResponse(responseCode = "200", description = "조회 성공",
                    content = @Content(schema = @Schema(implementation = EventListResponseDto.class)))})
    @GetMapping("/random")
    public ResultResponse<PagingResponse<TicketEventResponseDto>> getRandomEvents() {
        PagingResponse<TicketEventResponseDto> dto = eventService.findRandomOngoingEvents();
        return ResultResponse.of(ResultCode.EVENT_INFO_SUCCESS, dto);
    }

}