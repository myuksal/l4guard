# 정책 엔진과 hot reload 운영

이 문서는 `l4guard`의 정책 reload, 긴급 차단, listener 중지, quota 산정 방식을
운영자 관점에서 설명한다. 설계 기준은 `plan.md`, 작업 정의는
`workflow/epic.md`의 E4를 따른다.

## 1. Reload 절차

설정 파일을 수정한 뒤 실행 중인 프로세스에 `SIGHUP`을 보낸다.

```sh
kubectl edit configmap l4guard-policy
kubectl exec deploy/l4guard -- kill -HUP 1
```

1. 파일 전체를 다시 읽고 파싱·검증한다.
2. 검증에 성공하면 reload 가능한 설정만 한 번에 교체한다.
3. 교체 후 새 정책이 거부하는 기존 세션을 즉시 취소한다.
4. 파싱·검증·적용 중 하나라도 실패하면 마지막 정상 정책을 유지하고
   경고 로그만 남긴다.

`SIGHUP`은 정책 reload만 수행한다. 프로세스 종료는 `SIGINT`/`SIGTERM`을
사용한다.

## 2. Reload 가능 범위

다음 설정은 프로세스를 재시작하지 않고 적용된다.

| 설정 | 동작 |
|---|---|
| `policy.allow`, `policy.deny` | 새 연결 허용 여부와 기존 세션 취소 |
| `policy.emergency_block` | 전체 신규 차단과 전체 기존 세션 취소 |
| `limits.max_connections_per_ip` | 인바운드 IP별 동시 연결 상한 |
| `limits.max_new_connections_per_second` | listener별 신규 연결률 |
| `limits.new_connection_burst` | listener별 token bucket burst |
| `listeners[].enabled` | 특정 listener 중지·재개 |

다음 설정은 프로세스 수명 동안 고정이다. 변경하면 reload가 거부되고 마지막
정상 설정이 유지된다. 순차 재배포로 반영한다.

- `listeners[].name`, `listen`, `mode`, `upstream`
- `listeners[].l7` (HTTP L7 검사 설정 전체)
- `upstreams`
- `limits.max_connections`, `max_connections_per_listener`,
  `max_outbound_connections`
- `limits.connect_timeout_ms`, `drain_timeout_ms`
- `limits.max_sessions`, `max_ip_entries`, `ip_entry_ttl_seconds`
- `limits.bandwidth_limit_bytes_per_second`
- `observe`

연결 예산(semaphore)과 listener socket은 기동 시 고정되므로, 이를 바꾸려면
Guard를 재배포한다. 재배포 중 backend 프로세스와 데이터는 유지된다.

관측 설정(`observe`)과 지표·로그 상한은 재시작 대상이며, 상세 내용은
`docs/observe.md`에 기록한다.

## 3. 긴급 차단

특정 대역 또는 전체를 즉시 차단하려면 설정을 수정하고 `SIGHUP`을 보낸다.

```toml
[policy]
allow = ["0.0.0.0/0", "::/0"]
deny = ["203.0.113.0/24"]
emergency_block = false
```

전체 차단은 `emergency_block = true`로 바꾼다. 새 연결은 모두 거절되고,
등록된 모든 세션이 즉시 취소된다. 차단 해제도 같은 절차로 되돌린다.

`deny`는 `allow`보다 우선한다. IPv4와 IPv6 CIDR를 모두 지원하며, allow 목록이
비어 있으면 모든 주소를 거부한다(fail-closed).

## 4. Listener 단위 중지·재개

```toml
[[listeners]]
name = "cometbft-p2p-public"
listen = "0.0.0.0:26656"
upstream = "cometbft-p2p"
enabled = false
```

- `enabled = false`로 reload하면 해당 listener의 기존 세션을 취소하고,
  새로 accept되는 연결을 즉시 닫는다.
- socket은 계속 bind된 상태로 남는다. 다른 프로세스가 같은 포트를 차지하지
  못하게 하는 fail-closed 동작이다.
- `enabled = true`로 reload하면 별도 재기동 없이 다시 서비스한다.

## 5. 정책 버전

모든 정책 snapshot에는 `PolicyVersion`이 부여된다. 값은 주소 규칙, quota,
rate limit, listener 활성 상태를 정규화한 FNV-1a 64-bit digest다.

- 같은 설정을 읽은 복제본은 재시작 여부와 관계없이 같은 버전을 보고한다.
- 로그에는 `policy_version` 필드로, 종료 요약에는 `policy_version` 값으로
  노출된다.
- 복제본 간 버전이 다르면 같은 정책이 적용되지 않은 것이므로 reload 상태를
  확인한다. 이 값은 운영 비교용이며 암호학적 무결성 값이 아니다.

## 6. Quota는 복제본별이다

Guard의 연결 상한과 rate limit은 **프로세스(복제본)별**로 계산된다. 분산
카운터가 아니므로 backend가 실제로 받는 총량은 복제본 수만큼 커진다.

```text
backend 방향 최대 동시 연결
  = Guard 복제본 수 × max_connections

예: 2 복제본 × 256 = 최대 512
```

인바운드와 아웃바운드는 같은 프로세스 안에서 예산을 나눈다.

```text
인바운드 최대 동시 연결 = max_connections - max_outbound_connections
아웃바운드 최대 동시 연결 = max_outbound_connections
```

- `max_outbound_connections`는 고정 peer 재접속용으로 예약된다. 인바운드
  폭주가 이 슬롯을 모두 차지할 수 없다.
- `max_connections_per_listener`는 listener 하나의 상한이며 전역 상한보다
  클 수 없다.
- `max_connections_per_ip`는 인바운드 listener에만 적용된다. 아웃바운드는
  로컬 노드가 연결하므로 IP별 슬롯을 소비하지 않는다.
- `max_new_connections_per_second`와 `new_connection_burst`는 listener별
  token bucket이다. burst만큼 즉시 허용하고, 이후 초당 rate로 보충한다.
- 복제본 전체의 신규 연결 시도 상한은 대략
  `복제본 수 × max_new_connections_per_second`다. 순간 허용량은
  `복제본 수 × new_connection_burst`다.

## 7. 상태 map 상한

- 세션 registry는 `max_sessions`개를 넘지 않는다. 가득 차면 신규 연결을
  거절한다.
- IP별 상태 map은 `max_ip_entries`개를 넘지 않는다.
- `ip_entry_ttl_seconds`는 유휴 항목만 정리한다. 활성 연결 카운터
  (`active > 0`)는 TTL로 삭제하지 않는다. 삭제하면 IP별 상한을 우회할 수
  있기 때문이다.
- IP별 상한 검사는 listener별 rate bucket보다 먼저 수행한다. 이미 상한에
  도달한 IP가 다른 IP용 token을 소진하지 못하게 하기 위해서다.

## 8. 거절 사유 카운터

종료 요약과 `StatsSnapshot`에서 다음 값을 확인할 수 있다.

| 필드 | 의미 |
|---|---|
| `rejected_policy` | 주소 정책 또는 emergency block으로 거절 |
| `rejected_per_ip` | IP별 동시 연결 상한 또는 IP 상태 map 포화 |
| `rejected_rate` | listener별 신규 연결률 상한 |
| `rejected_budget` | 전역·listener별 연결 예산 포화 |
| `rejected_stopped` | 중지된 listener로 들어온 연결 |
| `rejected_sessions` | 세션 registry 포화 |

`cancelled`는 정책 변경·listener 중지·종료로 취소된 세션 수이고,
`completed`는 정상 종료된 세션 수다. `policy_version`은 현재 적용 중인
정책 버전이다.

## 9. 장애 시 동작

- 설정 파싱·검증 실패: 마지막 정상 정책 유지, 경고 로그.
- 고정 설정 변경: reload 거부, 마지막 정상 정책 유지, 경고 로그.
- reload와 신규 세션 등록 경합: 등록 시점에 현재 정책과 listener 상태를
  registry lock 안에서 다시 검사한다. 따라서 차단 대상이 등록 직후 정책
  변경으로 빠져나가는 경로가 없다.
- Guard 종료: 신규 accept 중지 → `drain_timeout_ms` 동안 정상 종료 대기 →
  남은 세션 취소 → 자원 회수. backend 프로세스와 데이터는 건드리지 않는다.
