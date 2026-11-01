# CometBFT through l4guard

이 디렉터리는 CometBFT backend가 Guard를 통해서만 고정 peer와 state-sync
RPC에 도달하도록 만드는 설정 예시를 담는다.

## 1. P2P 고정 peer

`config-patch.toml`을 backend의 `config.toml`에 적용한다.

```toml
filter_peers = false

[p2p]
external_address = "p2p.example.com:26656"
persistent_peers = "<peer-id>@l4guard-private.l4guard.svc.cluster.local:31001"
seeds = ""
pex = false
seed_mode = false
addr_book_strict = false
allow_duplicate_ip = true
```

핵심은 다음과 같다.

- `persistent_peers`는 실제 peer의 공인 주소가 아니라 Guard의 사설
  listener를 가리킨다. peer ID는 실제 peer 것을 유지한다.
- `external_address`는 Guard의 공개 P2P endpoint다. peer는 이 주소로
  접속하고 Guard가 backend로 중계한다.
- `pex = false`, `seeds = ""`로 직접 peer 탐색을 막는다.
- `addr_book_strict = false`로 Guard의 사설 주소를 주소록에 넣는다.
- backend는 모든 peer를 Guard IP로 보므로 `allow_duplicate_ip = true`가
  필요하다.
- `filter_peers = false`로 ABCI 앱의 주소 필터에 의존하지 않는다. 실제
  외부 주소 정책은 Guard의 `policy.allow`/`deny`가 담당한다.

`external_address`에 backend Pod IP를 넣으면 peer가 Guard를 우회해 backend로
직접 접속하려 한다. 반드시 공개 Guard endpoint를 넣는다.

## 2. state-sync RPC

`state-sync-rpc-hostaliases.yaml`은 backend Pod에만 provider hostname을
Guard 내부 Service로 해석하는 `hostAliases`를 추가한다.

```yaml
hostAliases:
  - ip: "203.0.113.50"   # l4guard/rpc-provider-a ClusterIP
    hostnames:
      - rpc.provider-a.example
```

- backend는 `rpc.provider-a.example`로 TLS를 시작하고 SNI·Host를 그대로
  유지한다.
- Guard는 `rpc.provider-a.example:443`을 실제 provider 주소로 해석해 TCP
  스트림을 중계한다. TLS를 종단하지 않으므로 인증서 검증은 backend와
  provider 사이에서 끝난다.
- `hostAliases`를 Guard Pod에 넣으면 Guard가 자기 자신의 Service로 연결하는
  순환이 생긴다. Guard에는 절대 적용하지 않는다.

## 3. 검증

```sh
# backend는 내부 Service IP를 본다
kubectl -n blockchain exec deploy/backend -- getent hosts rpc.provider-a.example

# Guard는 실제 provider 주소를 본다
kubectl -n l4guard exec deploy/l4guard-egress -- getent hosts rpc.provider-a.example

# backend -> Guard -> peer 경로
kubectl -n blockchain exec deploy/backend -- \
  nc -vz l4guard-private.l4guard.svc.cluster.local 31001
```

실제 block sync는 `scripts/e8-cometbft-sync.sh`로 로컬 Docker에서 재현할 수
있다. 이 스크립트는 validator 1개와 full node 1개를 만들고, full node가
Guard의 고정 egress listener를 통해 block을 동기화하는지 확인한다.
