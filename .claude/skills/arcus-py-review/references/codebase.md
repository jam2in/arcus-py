# 코드 구조와 비교 대상 (arcus-py)

jam2in/arcus-py develop `7b919f8`(2026-11, FEATURE: Add flush API) 기준이다. 이름과 위치는 바뀌었을 수 있으니 항상 grep으로 확인한다.
exptime은 64비트 signed다(`_validation.py`). watch 콜백은 cluster 락을 잡지 않는다(PR#18).

## 1. 한 줄 요약

ARCUS 캐시용 파이썬 드라이버다. ZooKeeper `/arcus/cache_list/<service_code>`에서 노드를 찾고, Ketama로 키의 노드를 골라, 노드별 풀의 연결로 memcached 텍스트 프로토콜 명령을 보낸다. 명령은 I/O 없는 generator(Exchange)이고, 같은 Exchange를 sync 소켓과 asyncio stream이 각각 구동한다.

```
sync:  Arcus.get(key) (client.py) → _encode_key → prepare_get (_command/kv.py) → Exchange
        → SyncCluster.execute(key, exchange)
            ├ 첫 요청: _initialize → _init_lock → ArcusDiscovery.start(_on_nodes_changed) → Routing
            ├ routing.pool_for(key) → ArcusLocator.get_node → SyncConnectionPool
            └ _run: pool.acquire() → SyncConnection.execute(exchange) → finally pool.release()
                    execute: next/send로 Write·ReadLine·ReadExactly 수행, 실패면 close(broken)
asyncio: AsyncCluster.execute(key, exchange)  (공개 AsyncArcus는 PR#26, 아직 미merge)
        → _raise_if_closed → _bind_loop → 첫 요청: _init_task(shield) → asyncio.to_thread(discovery.start)
        → pool_for(key) → await AsyncConnectionPool.acquire() (Semaphore) → await AsyncConnection.execute
        → finally pool.release() (동기)
노드 변경: kazoo 스레드 ChildrenWatch → ArcusDiscovery._handle_children → parse_cache_list
        → sync: _on_nodes_changed가 그 자리에서 Routing 교체 + 빠진 풀 close(abort_in_use)
        → asyncio: loop.call_soon_threadsafe(_apply_nodes) → 루프에서 같은 일
```

## 2. 모듈 지도

| 모듈 (경로) | 책임 / 핵심 식별자 | 하지 않는 것 (경계) |
|---|---|---|
| 공개 API `__init__.py`, `client.py` | `__all__`, `Arcus`(get/set/flush/vop_*/close, `with`), `_encode_key`, 공유 `_TRANSCODER` | 소켓·스레드 없음. 명령마다 `prepare_*` → `_cluster.execute` |
| 예외 `exceptions.py` | `ArcusError` 아래 Connection / OperationTimeout / PoolTimeout / Decode / Client / Server, `ArcusErrorGroup`(`derive` 재정의) | 의미가 공개 계약(SKILL.md, `checklist.md` 1.5) |
| 설정·검증 `_config.py`, `_validation.py` | `ClientConfig`(frozen, 생성 시 검증), `validate_key/vkey/exptime/int/prefix`, `MAX_KEY_LENGTH` 4000, `MAX_EXPTIME` 2^63-1 | 검증 함수는 여기에만 둔다(PR#22) |
| 프로토콜 `_protocol/` | `request.py`(`Write`/`ReadLine`/`ReadExactly`, `Exchange = Generator[Op, bytes, T]`), `framing.py`(`CRLF`, `read_data_block`), `status.py`(`raise_unexpected`) | 소켓, 시간, 스레드, asyncio를 만지지 않는다 |
| 명령 `_command/` | `kv.py`(get/set, flags 0), `flush.py`(flush_all/flush_prefix, `<null>`, factory 반환), `vector/__init__.py`(vop create/upsert/search/delete/drop, `VectorOptions`, `Metric`, `Quant`, `Vector` 핸들), `vector/_transcoder.py`(vkey·벡터·meta·filter 인코딩) | I/O 없음. 검증·인코딩은 `prepare_*`, 응답 해석은 `_xxx` generator |
| 값 `transcoder.py` | `ArcusTranscoder`: JSON(UTF-8), None 거절, 16KB 초과 gzip, decode는 gzip magic으로 판별 | flags를 보지 않는다 |
| sync 전송 `_transport/sync/` | `connection.py`(`SyncConnection`: open/execute/abort/close, 수신 버퍼, 응답 뒤 남은 바이트 검사), `pool.py`(`SyncConnectionPool`: `Condition`, `_size`, LIFO idle, `close(reason, abort_in_use)`), `cluster.py`(`SyncCluster`: 지연 초기화, `_init_lock`, fork 재초기화, `execute_all` + `ArcusErrorGroup`) | 연결은 한 번에 한 요청 |
| asyncio 전송 `_transport/aio/` | `connection.py`(`AsyncConnection`: `readuntil`/`readexactly`, 실패·취소면 abort, 남은 바이트 검사 없음), `pool.py`(`AsyncConnectionPool`: FIFO `Semaphore`, 동기 release/close), `cluster.py`(`AsyncCluster`: 루프 바인딩, shield한 init task, `to_thread`) | 스레드 락 없음. `execute_all` 없음 |
| 클러스터 `_cluster/` | `discovery.py`(`ArcusDiscovery`: KazooClient, `exists` 확인 뒤 `ChildrenWatch`, `parse_cache_list`), `locator.py`(`ArcusLocator`: 불변 Ketama ring, `key_hash`, `node_points`), `routing.py`(`Routing[P]`: locator + pools, `with_nodes`, `pools_missing_from`, `pool_for`) | Routing은 sync·asyncio 공용, 풀 타입만 다르다 |

## 3. 명령을 추가할 때 함께 바뀌는 곳

`7b919f8`(flush), `e75aa33`(vop delete/drop)이 견본이다. 한 곳이라도 빠지면 묻는다.

- `_command/<영역>.py`: `_xxx(...) -> Exchange[T]`(Write 한 번, 응답 줄마다 분기, 모르는 줄은 `raise_unexpected`)와 `prepare_xxx(...)`(검증·인코딩 후 `_xxx` 반환). 여러 노드로 보내면 factory를 반환한다(`prepare_flush`).
- `_validation.py`: 새 범위·길이 검증. 범위 상수는 명령 모듈의 `Final` range(`DIM_RANGE` 등).
- `client.py`: `Arcus.xxx` 메서드, 키는 `_encode_key`, 옵션은 키워드 전용. 핸들 객체가 있으면 위임 메서드(`Vector.upsert` 등)도.
- `__init__.py`: 새 공개 타입·enum·예외를 `__all__`에.
- asyncio: PR#26 이후 `aio.py`의 `AsyncArcus`에 같은 메서드. 전송 계층(`execute`/`execute_all`)에 새 기능이 필요하면 sync·aio cluster 양쪽.
- 테스트: `tests/support/response_cases.py`의 `CASES`(protocol·sync·asyncio 연결이 함께 돈다), `test_protocol.py` 또는 `test_vector_protocol.py`(요청 바이트, 경계값, `_before_iteration` 검증), `tests/support/fake_server.py`와 `test_client_sync.py`(클라이언트 경로).
- 문서는 따로 없다. 공개 메서드 docstring 한 줄이 문서다.

## 4. 비교 대상 ("X를 바꾸면 Y와 비교")

- **sync connection ↔ aio connection**: 예외 변환(Timeout → `ArcusOperationTimeoutError`, OSError·EOF → `ArcusConnectionError`), 실패 시 close/abort, `broken`, `_raise_if_broken`. sync에만 있는 `_raise_if_unread_data`는 asyncio에서 대신 무엇이 막는지 본다.
- **sync pool ↔ aio pool**: max_size, `pool_timeout`, LIFO idle, `close(reason, abort_in_use)`, 복사해 던지는 `_raise_if_closed`. 슬롯 회수 경로(`_size -= 1` ↔ `_slots.release()`)를 줄 단위로 맞춰 본다.
- **SyncCluster ↔ AsyncCluster**: 지연 초기화 공유(`_init_lock` ↔ shield한 `_init_task`), 실패 복사(`copy.copy(error) from error`), closed 확인 위치, 노드 변경(`_on_nodes_changed` ↔ `_apply_nodes`), close 순서(풀 close → init 대기 → discovery stop). fork는 sync만 pid로, asyncio는 루프 바인딩으로 막는다.
- **명령끼리**: 응답을 `True/False/None`으로 나누는 기준, `read_data_block`으로 읽는 data block, `ArcusDecodeError`로 감싸는 디코드(`kv._get`, `vector.decode_meta`).
- **locator**: `node_points`(40 × `<4I`), `key_hash`(`<I`), 충돌 시 작은 주소, `bisect_left` wrap. `tests/test_locator.py`의 기대값.
- **discovery**: start 실패 시 `_cleanup`과 예외 변환(타임아웃/NoNode/Kazoo·OSError), 첫 목록은 start 안에서 콜백으로 전달(PR#18), `_handle_children`은 예외를 로그로 삼킨다.
- **설정**: `ClientConfig` 기본값 ↔ `Arcus.__init__` 기본값 ↔ `_create_pool`/`_open_connection`이 넘기는 값.

## 5. 항상 grep으로 확인

- 짝 찾기: `grep -rn "def <이름>" src/arcus/_transport/` 로 sync·aio 양쪽을 연다.
- 명령 등록: `grep -rn "prepare_" src/arcus/client.py tests/support/response_cases.py` 로 새 `prepare_xxx`가 클라이언트와 `CASES`에 모두 있는지.
- 공개 이름: `grep -n "__all__" -A20 src/arcus/__init__.py`.
- 풀 회수: `grep -n "_size\|_slots" src/arcus/_transport/*/pool.py`.
- 응답 줄: `grep -rn 'b"END"\|raise_unexpected' src/arcus/_command/` 로 새 Exchange가 끝 줄을 읽는지.
- 스레드 경계: `grep -rn "to_thread\|call_soon_threadsafe\|Lock()" src/arcus/`.
- 남은 TODO: `grep -rn "TODO" src/` (지금은 `client.py`의 키 검증 공유 하나).
