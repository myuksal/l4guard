# 관측과 제한된 로깅

이 문서는 `l4guard`의 지표 정의, 로그 상한, 구조화 로그, 노출 방식,
민감정보 처리 기준을 운영자 관점에서 설명한다. 설계 기준은 `plan.md`,
작업 정의는 `workflow/epic.md`의 E5를 따른다.

## 1. 지표

모든 카운터는 프로세스 시작 이후 누적값이며, Guard를 재시작하면 0부터 다시
센다. Guard는 상태를 저장하지 않으므로 장기 추세는 관측 시스템에서 집계한다.
Quota와 마찬가지로 지표도 **복제본별 값**이므로, 전체 트래픽은 복제본 합으로
계산한다.

| 지표 | 종류 | 의미 |
|---|---|---|
| `l4guard_active_sessions` | gauge | 현재 등록되어 중계 중인 세션 수 |
| `l4guard_accepted_total` | counter | 정책과 예산 검사를 통과한 세션 수 |
| `l4guard_rejected_total{reason="policy"}` | counter | 주소 정책으로 거절된 연결 |
| `l4guard_rejected_total{reason="budget"}` | counter | 연결 예산이 없어 거절된 연결 |
| `l4guard_rejected_total{reason="per_ip"}` | counter | IP별 동시 연결 상한으로 거절된 연결 |
| `l4guard_rejected_total{reason="rate"}` | counter | 신규 연결률 상한으로 거절된 연결 |
| `l4guard_rejected_total{reason="stopped"}` | counter | 중지된 listener로 거절된 연결 |
| `l4guard_rejected_total{reason="sessions"}` | counter | 세션 registry가 가득 차 거절된 연결 |
| `l4guard_upstream_failures_total{reason="connect"}` | counter | upstream 연결 실패 |
| `l4guard_upstream_failures_total{reason="timeout"}` | counter | upstream 연결 deadline 초과 |
| `l4guard_copy_failures_total` | counter | 복사 오류로 끝난 세션 |
| `l4guard_cancelled_total` | counter | 정책·listener 중지·종료로 취소된 세션 |
| `l4guard_completed_total` | counter | 정상 종료한 세션 |
| `l4guard_accept_errors_total` | counter | `accept` 호출이 실패한 횟수 |
| `l4guard_bytes_total{direction="upstream"}` | counter | client → upstream 전송 바이트 |
| `l4guard_bytes_total{direction="client"}` | counter | upstream → client 전송 바이트 |
| `l4guard_policy_version` | gauge | 현재 적용된 정책 snapshot의 버전 |
| `l4guard_l7_requests_total` | counter | L7 listener가 검사한 HTTP 요청 수 |
| `l4guard_l7_forwarded_total` | counter | L7 검사를 통과해 upstream으로 전달한 요청 수 |
| `l4guard_l7_rejected_total{reason="..."}` | counter | 사유별 L7 요청 거부 수 |

L7 거부 사유는 `malformed`, `header`, `body`, `timeout`, `method`,
`encoding`, `upgrade`, `json`, `policy`, `quota`다. 연결 거부 카운터와 단위가
다르므로 `l4guard_rejected_total`과 분리해 집계한다. 상세 내용은
`docs/l7.md`에 기록한다.

`l4guard_policy_version`은 정책과 quota의 입장 관련 설정으로 계산한 결정적
digest다. 같은 설정을 쓰는 복제본은 재시작 여부와 관계없이 같은 값을
보고하므로, 모든 Guard가 동일 정책을 적용했는지 비교할 수 있다.

## 2. 로그 상한

로그 폭주가 Guard 자체를 멈추게 하지 않도록 두 단계로 제한한다.

```toml
[observe]
level = "info"
log_rate_limit_per_second = 100
log_queue_capacity = 1024
```

- `log_rate_limit_per_second`: 1초 고정 창에서 허용하는 로그 이벤트 수.
- `log_queue_capacity`: writer thread가 소비하기 전에 대기할 수 있는 이벤트 수.

두 상한 중 하나를 넘은 이벤트는 버리고 개수만 센다. 버린 이벤트가 있으면
writer가 **최대 초당 1회** 다음 형식의 요약 줄을 stderr에 남긴다.

```
l4guard: suppressed 812 log event(s) by observe limits
```

요약 줄은 `tracing`을 거치지 않으므로 rate limit에 다시 걸리지 않는다.
거절 폭주 시에는 개별 거절 로그 대신 카운터와 요약 줄로 상황을 판단한다.
`level`을 `debug`로 올리면 거절 사유별 개별 로그를 볼 수 있지만, 상한은
그대로 적용된다.

## 3. 구조화 로그

로그는 `tracing` 필드로 다음 값을 남긴다.

| 필드 | 사용 위치 | 의미 |
|---|---|---|
| `listener` | listener·세션·거절 로그 | 설정의 listener 이름 |
| `peer` | 세션·거절 로그 | Guard가 관측한 peer 주소 |
| `reason` | 거절·세션 실패 로그 | `policy`, `budget`, `per_ip`, `rate`, `stopped`, `sessions`, `connect_failed`, `connect_timeout`, `copy`, `cancelled` |
| `version` | 거절·세션·reload 로그 | 정책 버전 |
| `session` | 세션 로그 | registry가 발급한 세션 ID |

reload 로그는 `previous_version`, `version`, `cancelled`, `stopped`,
`resumed` 필드를 사용한다. 거절 로그의 메시지는 모두 `connection rejected`로
같고 사유는 `reason` 필드로 구분한다.

## 4. 노출 방식

기본 노출은 stderr 로그다. 기존 로그 수집기가 있으면 별도 설정 없이 붙일 수
있다. Prometheus 형식이 필요하면 endpoint를 선택적으로 켠다.

```toml
[observe]
metrics_listen = "127.0.0.1:9100"
```

```sh
curl -s http://127.0.0.1:9100/metrics
```

- 경로는 `GET /metrics` 하나다. 다른 경로와 메서드는 `404`를 반환한다.
- 응답은 Prometheus text 형식이며, 지표 정의 외의 정보는 포함하지 않는다.
- 동시에 처리하는 요청은 8개로 제한하고, 요청은 1 KiB·2초 안에서만 읽는다.
  상한을 넘는 연결은 대기시키지 않고 닫는다.
- 공개 listener와 분리해 loopback 또는 pod 내부 주소에 bind하고, 기존
  관측 경로(ServiceMonitor, scrape annotation 등)로 수집한다.
- `observe`는 재시작 대상 설정이다. endpoint 주소나 로그 상한을 바꾸면
  `SIGHUP` reload는 거부되고 마지막 정상 설정이 유지된다. 순차 재배포로
  반영한다.

endpoint를 켜지 않아도 종료 시 전체 카운터와 `policy_version`을 구조화
로그로 남긴다. 다만 로그 상한에 걸린 이벤트는 요약 줄로 대체되므로, 종료
요약을 반드시 보존해야 하면 `log_rate_limit_per_second`를 충분히 크게 두거나
endpoint를 사용한다.

## 5. 민감정보

- 설정·정책 문서 전체를 덤프하지 않는다. startup 요약은 listener·upstream
  주소와 allow/deny **개수**만 출력하고, CIDR 목록이나 원문은 남기지 않는다.
- metrics 응답에는 설정값, CIDR, peer 주소 목록을 넣지 않는다.
- `peer` 주소는 IP 정책과 quota 판단에 필요하므로 세션·거절 로그에 남긴다.
  주소를 민감정보로 취급하는 환경에서는 로그 수집 단계에서 마스킹한다.
- upstream 호스트 이름과 내부 주소는 startup 요약에만 나오며, 반복 로그에는
  남기지 않는다.
- `debug` 수준은 연결 단위 로그를 남기므로 운영 기본값(`info`)으로 두고,
  문제 분석 기간에만 일시적으로 올린다.
