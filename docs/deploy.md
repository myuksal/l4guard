# 배포와 접근 통제

이 문서는 `l4guard`의 Kubernetes 배포 자산과 backend 우회 차단 절차를
설명한다. 설계 기준은 `plan.md` 3·10장, 작업 정의는 `workflow/epic.md`의
E7을 따른다.

## 1. 자산 구성

```text
Dockerfile                     # release 이미지 (non-root, read-only rootfs)
.dockerignore
deploy/base/                   # Guard 네임스페이스 자산
  namespace.yaml
  serviceaccount.yaml
  configmap-p2p.yaml           # 공개 P2P listener 설정
  configmap-rpc.yaml           # L7 RPC listener 설정
  configmap-egress.yaml        # backend -> 고정 peer 사설 listener 설정
  deployment-p2p.yaml          # 리스너별 Deployment
  deployment-rpc.yaml
  deployment-egress.yaml
  service-p2p.yaml             # 공개 P2P LoadBalancer
  service-rpc.yaml             # 공개 RPC LoadBalancer
  service-private.yaml         # backend -> Guard 사설 ClusterIP
  service-rpc-egress.yaml      # state-sync RPC provider별 ClusterIP
  service-metrics.yaml         # Prometheus / probe endpoint
  networkpolicy.yaml           # Guard ingress/egress
  pdb-p2p.yaml
  pdb-rpc.yaml
  pdb-egress.yaml
deploy/backend/                # backend 네임스페이스 자산
  namespace.yaml
  service.yaml                 # ClusterIP only
  networkpolicy.yaml           # Guard 출처만 허용
deploy/chains/                 # CometBFT/Geth backend 설정 예시
  cometbft/
  geth/
```

공개 P2P, 공개 RPC, 사설 egress는 **별도 Deployment**다. 한 체인의 P2P
부하가 RPC 응답이나 backend의 고정 peer 경로에 영향을 주지 않게 하고,
교체·자원 예산을 분리하기 위해서다. 같은 바이너리를 설정만 바꿔 재사용한다.

`--check-config <path>`는 소켓을 열지 않고 설정 파일만 검증한다. CI, init
container, 배포 전 검사에 사용한다.

```sh
l4guard --check-config /etc/l4guard/guard.toml
```

## 2. 사전 준비

### Guard 전용 워커

Guard Pod는 backend와 다른 워커에 배치한다. 커널 장애 범위를 분리하기
위해서다.

```sh
kubectl label node guard-a l4guard.io/role=guard
kubectl label node guard-b l4guard.io/role=guard
kubectl taint node guard-a l4guard.io/guard=true:NoSchedule
kubectl taint node guard-b l4guard.io/guard=true:NoSchedule
```

- `deployment-*.yaml`은 `nodeSelector: l4guard.io/role=guard`와 taint
  toleration을 사용한다.
- backend 워커는 사설 네트워크에 두고, 상위 방화벽에서 외부 진입을
  차단한다. backend 워커에는 Guard Pod가 스케줄되지 않게 한다.
- Guard 워커가 정확히 두 대라면 `replicas: 2`와 required pod anti-affinity를
  유지한다. 세 번째 Pod를 만들 수 없으므로 rolling update는
  `maxSurge: 0`, `maxUnavailable: 1`로 순차 교체한다.

### 이미지

```sh
docker build -t ghcr.io/example/l4guard:0.1.0 .
docker push ghcr.io/example/l4guard:0.1.0
```

매니페스트의 `image:`를 실제 저장소로 바꾼다. 이미지는 UID/GID `65532`로
실행되고 root filesystem은 읽기 전용이다. 설정 파일은 ConfigMap에서
`/etc/l4guard`에 read-only로 mount한다.

### 네임스페이스와 라벨

- Guard: `l4guard` 네임스페이스, `app.kubernetes.io/name=l4guard`
- Backend: `blockchain` 네임스페이스, `app.kubernetes.io/name=blockchain-node`

기존 backend 네임스페이스를 쓰면 `deploy/backend/networkpolicy.yaml`의
`namespace`, `podSelector`를 실제 라벨에 맞춘다.

## 3. 배포 순서

```sh
# 1. backend 접근 정책과 ClusterIP Service
kubectl apply -k deploy/backend

# 2. Guard namespace, ConfigMap, Deployment, Service, NetworkPolicy, PDB
kubectl apply -k deploy/base

# 3. 상태 확인
kubectl -n l4guard rollout status deploy/l4guard-p2p
kubectl -n l4guard rollout status deploy/l4guard-rpc
kubectl -n l4guard rollout status deploy/l4guard-egress
kubectl -n l4guard get svc,pod,networkpolicy,pdb
```

렌더링만 확인하려면:

```sh
kubectl kustomize deploy/base
kubectl kustomize deploy/backend
kubectl apply --dry-run=client -k deploy/base
```

## 4. 노출과 접근 통제

### LB는 Guard 워커만 대상으로

`service-p2p.yaml`, `service-rpc.yaml`은 다음을 사용한다.

- `type: LoadBalancer`
- `externalTrafficPolicy: Local`
- Guard Pod selector

`externalTrafficPolicy: Local`은 두 가지를 보장한다.

1. LB가 Guard Pod가 있는 워커로만 트래픽을 보낸다.
2. Guard가 client source IP를 볼 수 있어 주소 정책이 의미를 가진다.

`Local` 모드에서는 LB health check가 각 노드의 `healthCheckNodePort`를
사용한다. Guard Pod가 없는 노드는 unhealthy로 제거되므로, backend 워커가
target group에 잠시 포함되어도 실제 트래픽을 받지 않는다. 그래도 target
group 자체를 Guard 워커로 제한하는 편이 안전하다.

클라우드별로 LB target을 Guard 워커로 더 좁힌다.

- AWS Load Balancer Controller:
  `service.beta.kubernetes.io/aws-load-balancer-target-node-labels: l4guard.io/role=guard`
- GCP: Guard 워커만 포함한 zonal NEG 또는 instance group 사용

앞단 LB가 TCP 연결을 종단해 source IP를 잃으면, Guard는 LB 주소만 본다.
이 경우 PROXY protocol 수신 여부를 별도로 결정한다(E8-T7). 신뢰하지 않는
경로에서 PROXY protocol을 받아서는 안 된다.

### backend는 ClusterIP만

`deploy/backend/service.yaml`은 `type: ClusterIP`다. 다음은 금지한다.

- backend Service를 `NodePort` 또는 `LoadBalancer`로 바꾸는 것
- backend Pod에 `hostPort`를 두는 것
- backend 워커를 LB target group에 넣는 것

하나라도 적용되면 외부에서 backend로 직통하는 경로가 생기고 fail-closed
설계가 깨진다.

### NetworkPolicy

Guard(`deploy/base/networkpolicy.yaml`):

- 공개 26656/26657 ingress는 LB/peer에서 허용한다. 실제 peer 필터는 Guard의
  `policy.allow`/`deny`가 담당한다.
- 사설 31001/31002(P2P)와 32001(state-sync RPC) ingress는 `blockchain`
  네임스페이스의 backend Pod만 허용한다.
- 9100은 monitoring 네임스페이스만 허용한다.
- egress는 DNS, backend의 26656/26657, 고정 P2P peer와 state-sync RPC
  provider만 허용한다. peer/provider CIDR을 알면 `0.0.0.0/0`을 그 CIDR로
  좁힌다.

일부 CNI는 egress 정책을 DNAT 전에 평가한다. Guard가 backend Service
ClusterIP로 연결하지 못하면, backend Pod CIDR 또는 Service CIDR을 egress
`ipBlock`에 추가한다.

Backend(`deploy/backend/networkpolicy.yaml`):

- ingress는 Guard Pod의 26656/26657과 monitoring의 9100만 허용한다.
- egress는 DNS와 Guard 사설 listener 31001/31002/32001만 허용한다.
- 그 밖의 내부 의존성은 주석 위치에 **명시적으로** 추가한다. default
  allow-all egress를 넣지 않는다.

NetworkPolicy는 기존 연결을 즉시 끊는 것을 보장하지 않는다. 긴급 차단은
Guard의 `policy.deny`/`emergency_block`과 listener `enabled=false`로 수행한다.
`docs/policy.md`를 따른다.

### fail-closed 점검표

배포 후 다음을 확인한다.

- [ ] backend Service가 `ClusterIP`이고 `NodePort`가 없다.
- [ ] backend Pod에 `hostPort`가 없다.
- [ ] backend 워커의 외부 IP:26656/26657이 외부에서 닫혀 있다.
- [ ] Guard 워커에만 LB target이 등록되어 있다.
- [ ] backend NetworkPolicy가 Guard 출처 외 ingress를 거부한다.
- [ ] Guard 장애 시 backend 직통 경로를 여는 자동화가 없다.
- [ ] 공개 endpoint 하나에 backend Node ID 하나만 연결되어 있다.

## 5. Readiness와 liveness 분리

Guard는 `observe.metrics_listen`에서 `/metrics`와 함께 `/healthz`,
`/readyz`를 제공한다.

- `/healthz`: 프로세스와 listener가 살아 있음
- `/readyz`: 새 연결을 받을 준비가 됨
- 두 경로 모두 **backend 상태를 확인하지 않는다.**

Deployment probe는 `curl`로 `127.0.0.1:9100`을 호출한다. loopback이므로
NetworkPolicy와 무관하고, backend readiness를 기다리지 않는다.

Backend의 readiness는 backend가 직접 정의한다. Guard readiness가 backend
readiness를 기다리거나, backend 시작이 Guard readiness를 기다리는 순환을
만들지 않는다. Guard는 backend가 죽어도 listener를 유지하고 새 연결의
upstream 연결 실패만 기록한다.

## 6. 교체와 reload

### Rolling update

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 0
    maxUnavailable: 1
```

- `maxSurge: 0`: 새 Pod를 먼저 만들지 않는다. Guard 워커가 두 대뿐일 때
  스케줄 실패를 피한다.
- `maxUnavailable: 1`: 한 번에 한 Pod만 교체한다.
- `minReadySeconds: 5`, `preStop: sleep 5`: LB endpoint 갱신 시간을 준다.
- `terminationGracePeriodSeconds: 30` > `limits.drain_timeout_ms` (15초).
  drain이 끝난 뒤 SIGKILL까지 여유를 둔다.
- PDB `minAvailable: 1`: 자발적 중단에서 최소 한 Guard를 유지한다.

교체 중 기존 TCP 세션은 새 Pod로 이동하지 않는다. backend 프로세스와
데이터는 유지되고, peer는 정상 Guard를 통해 재접속한다. peer 수와 동기화
지연의 회복 시간을 측정한다.

### 정책 reload

reload 가능한 설정은 `policy.*`, IP별 상한, 신규 연결률, listener
`enabled`다. ConfigMap을 수정한 뒤 kubelet이 파일을 갱신할 시간을 두고
SIGHUP을 보낸다.

```sh
kubectl -n l4guard exec deploy/l4guard-p2p -- kill -HUP 1
kubectl -n l4guard logs deploy/l4guard-p2p --tail=20
```

실패하면 마지막 정상 정책이 유지되고 경고 로그가 남는다. listener 주소,
upstream, 연결 예산, `observe`, L7 설정 변경은 재시작 대상이므로
`kubectl rollout restart` 또는 이미지 교체로 반영한다.

ConfigMap 갱신 지연을 피하려면 `kubectl rollout restart`를 사용할 수 있다.
다만 이는 세션을 재설정하므로 긴급 정책 변경에는 SIGHUP을 우선한다.

## 7. 자원 예산과 측정

초기 실험값은 Guard Pod 하나 기준이다.

| 항목 | request | limit |
|---|---:|---:|
| CPU | 100m | 1 |
| Memory | 64 MiB | 256 MiB |
| 동시 연결 | 256 | 512·1,024까지 시험 |

매니페스트에 같은 값이 들어 있다. 방향당 8 KiB 복사 버퍼만으로 사양을
정하지 않는다. 다음을 함께 측정한다.

- cgroup memory: `container_memory_working_set_bytes`
- FD 수: `container_fs_usage_bytes`, 프로세스 `fd` 사용량
- socket memory: `node_netstat_Tcp_Mem`, `ss -m`
- conntrack: `node_nf_conntrack_entries`, `nf_conntrack_max`
- CPU throttling: `container_cpu_cfs_throttled_seconds_total`

Guard 자체가 작아도 커널 격리를 위한 전용 워커 비용은 별도다.

## 8. 하나의 공개 endpoint = 하나의 Node ID

공개 P2P endpoint 뒤에는 backend Node ID 하나만 둔다.

- Guard replica 두 개가 같은 backend로 중계하는 것은 가능하다.
- 서로 다른 Node ID의 backend 여러 개를 같은 endpoint 뒤에 무작위 분산하지
  않는다. CometBFT는 접속 주소의 peer ID와 실제 인증 키를 확인한다.

매니페스트는 `l4guard.io/backend-node-id` annotation으로 이 관계를
기록한다. 새 체인이나 새 backend는 별도 공개 endpoint와 별도 Deployment로
추가한다.

## 9. 검증

```sh
# 매니페스트 렌더링과 client dry-run
kubectl kustomize deploy/base >/dev/null
kubectl apply --dry-run=client -k deploy/base

# 설정 파일 검증
kubectl -n l4guard exec deploy/l4guard-p2p -- \
  /usr/local/bin/l4guard --check-config /etc/l4guard/guard.toml

# probe
kubectl -n l4guard port-forward svc/l4guard-metrics 9100:9100
curl -fsS http://127.0.0.1:9100/healthz
curl -fsS http://127.0.0.1:9100/readyz

# backend 직통 경로가 없는지 외부에서 확인
nc -vz <backend-worker-ip> 26656   # 실패해야 한다
nc -vz <backend-worker-ip> 26657   # 실패해야 한다

# Service 타입 확인
kubectl -n blockchain get svc blockchain-node -o jsonpath='{.spec.type}'
# ClusterIP
```

## 10. E7 작업 대응

| ID | 자산/문서 |
|---|---|
| E7-T1 | `deployment-p2p.yaml`, `deployment-rpc.yaml`, `deployment-egress.yaml`, `maxSurge: 0`, `maxUnavailable: 1` |
| E7-T2 | `service-*.yaml`의 `externalTrafficPolicy: Local`, Guard 전용 워커 라벨, backend ClusterIP |
| E7-T3 | `deploy/backend/networkpolicy.yaml` |
| E7-T4 | backend Service ClusterIP 고정, Guard 전용 LB target, fail-closed 점검표 |
| E7-T5 | `/healthz`, `/readyz`, backend 비의존 probe |
| E7-T6 | Deployment resources, 7장 측정 항목 |
| E7-T7 | `l4guard.io/backend-node-id` annotation, 8장 |
