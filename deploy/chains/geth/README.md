# Geth through l4guard

이 디렉터리는 Geth backend가 Guard를 통해서만 고정 peer와 동기화하도록 만드는
설정 예시를 담는다. 초기 범위는 **TCP 고정 peer + discovery 비활성**이다.
UDP discovery는 Guard가 중계하지 않는다.

## 1. 실행 flag

```sh
geth \
  --nodiscover \
  --netrestrict 10.0.0.0/8,172.16.0.0/12,192.168.0.0/16 \
  --maxpeers 10 \
  --syncmode snap \
  --port 30303 \
  --nat extip:<guard-public-ip> \
  --bootnodes ""
```

| flag | 이유 |
|---|---|
| `--nodiscover` | UDP discovery를 끄고 고정 peer만 사용한다. Guard는 TCP만 중계한다. |
| `--netrestrict` | Guard 사설 Service와 내부 의존성 CIDR만 허용한다. |
| `--maxpeers` | 고정 peer 수를 명시적으로 제한한다. |
| `--syncmode snap` | snap sync 경로를 검증한다. |
| `--nat extip:<guard-public-ip>` | enode/ENR이 backend 사설 IP가 아니라 Guard 공개 주소를 광고하게 한다. |
| `--bootnodes ""` | discovery bootnode 경로를 제거한다. |

`--nat extip`에는 Guard의 공개 **IP**를 넣는다. hostname을 쓰는 LB라면 그
hostname이 해석하는 공개 IP를 넣는다. backend Pod IP를 넣으면 peer가 Guard를
우회한다.

## 2. static peer

`static-nodes.json`을 Geth datadir에 둔다. 기본 경로는
`<datadir>/geth/static-nodes.json`이다.

```json
[
  "enode://<peer-pubkey>@l4guard-private.l4guard.svc.cluster.local:31001"
]
```

- enode의 공개 키는 실제 peer 것을 유지한다.
- IP·port만 Guard의 사설 listener로 바꾼다.
- Guard의 egress listener upstream은 실제 peer의 `30303`을 가리켜야 한다.
  체인에 따라 포트가 다르면 `configmap-egress.yaml`도 함께 바꾼다.

## 3. 검증

```sh
# backend에서 Guard 사설 listener까지
kubectl -n blockchain exec deploy/backend -- \
  nc -vz l4guard-private.l4guard.svc.cluster.local 31001

# Guard 로그에서 outbound 세션 확인
kubectl -n l4guard logs deploy/l4guard-egress --tail=50

# snap sync 상태
kubectl -n blockchain exec deploy/backend -- \
  geth attach --exec 'eth.syncing'
```

`eth.syncing`이 `false`가 되고 `eth.blockNumber`가 증가하면 동기화가
완료된 것이다. 실제 snap sync는 상태가 있는 peer가 필요하므로 운영 환경의
검증 절차로 수행한다.

## 4. 주의

- discovery를 끄지 않으면 Geth가 UDP로 직접 peer를 찾으려 하고, Guard를
  우회하는 경로가 생길 수 있다.
- `--netrestrict`에 Guard 사설 Service CIDR이 빠지면 고정 peer 연결이
  실패한다.
- enode/ENR에 backend 사설 IP가 광고되면 peer가 Guard를 우회한다.
- Geth의 IP 기반 inbound 제한은 Guard IP 기준으로 동작한다. 실제 외부 IP
  정책은 Guard에서 담당한다.
