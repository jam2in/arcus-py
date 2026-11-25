# 리뷰 관점과 규칙 (arcus-py)

jam2in/arcus-py의 리뷰 코멘트(2026-10 ~ 2026-11, f1v3-dev와 ing-eoking의 코멘트 45개)와 팀이 정해 둔 코딩 규칙("팀 규칙"으로 표시)에서 추렸다.
괄호 안 `PR#n`은 근거가 된 PR이다. 원본 정리는 `corpus/distill/arcus-py.md`에 있다.
같은 서버·프로토콜을 쓰는 Java 클라이언트의 리뷰 기준(`skills/arcus-java-client-review`)에서 이 코드에 맞는 것만 가져와 "java-client 기준에서 가져옴"으로 표시했다.

이 프로젝트는 엄격도가 낮다(SKILL.md "엄격도"). 아래 1절은 우선순위 순서다. 앞쪽일수록 놓치면 안 된다.

---

## 1. 보는 것

### 1.1 응답 경계와 연결 재사용
- Exchange는 응답을 끝(`END`, 상태 한 줄)까지 읽고 return한다. 한 연결은 요청을 순서대로 처리하므로 남은 줄은 다음 요청의 응답으로 읽힌다. (java-client 기준에서 가져옴)
  - asyncio 연결은 응답 뒤 남은 바이트를 검사하지 않는다(PR#17, `StreamReader`가 버퍼를 공개하지 않음). sync도 이미 받은 바이트만 검사한다. 끝까지 읽지 않는 Exchange는 테스트를 통과해도 운영에서 응답이 밀린다.
- 응답 줄 수와 token 수를 형식대로 따진다. `VALUE <key> <flags> <bytes>`, `VECTORS <n>` 뒤 n줄과 `END`, `withmeta`면 줄마다 data block. data block은 `read_data_block`으로 읽어 CRLF 종료를 확인한다. (java-client 기준에서 가져옴)
- 파싱 실패(`int()`, `float()`, unpack, token 수)는 `ArcusConnectionError`로 바꾼다. `ValueError`/`IndexError`가 그대로 새면 사용자는 프로토콜 위반을 입력 오류로 본다. 모르는 줄은 `raise_unexpected(line)`로 넘긴다(CLIENT_ERROR/SERVER_ERROR/ERROR 분류가 거기 있다).
- 실패하거나 취소된 연결은 `broken`이 되어 풀로 돌아가지 않아야 한다. 새 경로가 예외를 삼키고 연결을 재사용 가능 상태로 반납하면 P1이다. 반대로 응답을 끝까지 읽은 뒤의 디코드 실패로 연결을 버리는 것은 알려진 문제다(PR#10 기록용). 이번 PR이 넓히지 않으면 지적하지 않는다.
- Exchange 객체는 한 번만 돈다. 여러 노드나 재시도에는 factory로 새로 만든다(`execute_all`, `prepare_flush`). (PR#29)

### 1.2 풀 슬롯과 수명 (정확히 한 번)
- `acquire()` 하나에 `release()` 하나다. cluster의 `_run`/`execute`처럼 `try/finally`로 묶였는지 본다. (java-client 기준에서 가져옴: 완료·cancel은 모든 경로에서 정확히 한 번)
- sync 풀의 `_size`, asyncio 풀의 `Semaphore` 슬롯이 모든 경로(접속 실패, 접속 중 close, 대기 중 취소, broken 반납)에서 한 번씩 돌아오는지 센다. 새면 그 노드는 결국 `ArcusPoolTimeoutError`만 낸다(P1).
- `close()`는 여러 번 불러도 안전해야 하고, 대기자를 모두 깨워야 한다. asyncio 풀은 슬롯 하나를 풀어 대기자가 차례로 깨어나는 구조다. 이 연쇄를 끊는 return이 없는지 본다.
- 닫는 이유는 예외로 넘긴다(노드 제거 = `ArcusConnectionError`, 클라이언트 종료 = `ArcusError("client is closed")`). 대기자에게는 `copy.copy`로 나눠 주므로 `from error`로 원인을 붙인 예외를 넘기면 원인이 빠진다. (PR#7)

### 1.3 스레드, 이벤트 루프, fork 경계
- kazoo watch 콜백(`_on_nodes_changed`)은 kazoo 스레드에서 하나씩 돈다. sync는 그 자리에서 `Routing`을 통째로 바꾸고, asyncio는 `call_soon_threadsafe`로 루프에 넘기기만 한다. kazoo 스레드에서 asyncio 객체(풀, task)를 직접 만지면 P1이다. (java-client 기준에서 가져옴: watcher는 알림만, 반영은 클라이언트 쪽)
- 콜백에서 락을 잡거나 I/O·블로킹 호출을 하지 않는다(PR#18에서 콜백의 락을 뺐다). `_start_discovery_locked`는 `_init_lock`을 잡은 채 `discovery.stop()`을 부르므로, 콜백이 그 락을 기다리게 바꾸면 교착할 수 있다. (팀 규칙)
- asyncio 경로에서 ZooKeeper 호출과 블로킹 소켓 호출은 `asyncio.to_thread`로 보낸다. ruff `ASYNC`는 `time.sleep`·`open` 같은 일부만 잡고 kazoo·socket 호출은 못 잡는다.
- 락을 잡은 채 I/O, 블로킹 호출, 사용자·외부 callback을 부르지 않는다. 필요한 상태를 락 안에서 복사하고 락 밖에서 처리한다. 여러 스레드가 바꾸는 상태에는 어떤 락이 지키는지 적혀 있어야 하고, 모듈 수준 가변 전역은 두지 않는다. (팀 규칙)
- 요청 경로가 읽는 공유 상태는 불변 객체를 참조째 바꾼다(`Routing`, `ArcusLocator`). 부분 갱신 API를 되살리지 않는다. (PR#15)
- `_closed` 확인은 routing을 읽은 뒤에 한다. close 뒤 늦게 도착한 routing을 쓰지 않기 위해서다(`SyncCluster._current_routing`). 순서를 바꾸는 변경을 본다. (PR#18)
- fork: sync cluster는 pid가 바뀌면 부모의 discovery·소켓·락을 닫지 않고 버린다. 자식이 부모의 ZooKeeper 세션이나 소켓을 stop/close/shutdown하면 P1이다. asyncio cluster는 첫 요청의 루프에 묶이고, 루프 검사는 `pool.acquire()`보다 먼저 한다. (PR#13, PR#21)

### 1.4 취소와 타임아웃 (asyncio)
- 요청 중 취소되면 연결을 abort한다(`execute`의 `except BaseException`). 새 `await`를 넣은 곳이 취소돼도 연결과 슬롯이 정리되는지 본다.
- `release()`와 `close()`는 취소되지 않도록 동기 메서드다(PR#19). 여기에 `await`를 넣으면 정리 도중 취소될 수 있다.
- 공유 초기화 task는 `asyncio.shield`로 기다린다. 한 요청의 취소가 다른 요청의 초기화를 멈추면 안 된다. (PR#21)
- 상대에 달린 await(`wait_closed`, `drain`)는 끝나지 않을 수 있다. timeout이나 abort 경로가 있는지 본다. (PR#17)
- 모든 소켓 대기에는 `socket_timeout`/`connect_timeout`/`pool_timeout`이 걸린다. 무한 대기 경로를 새로 만들지 않는다. (java-client 기준에서 가져옴: 무한 대기 금지)

### 1.5 예외의 의미와 인자 검증
- 예외 의미는 공개 계약이다(`exceptions.py`). `ArcusConnectionError`는 노드에 닿지 못했거나 연결이 깨짐, `ArcusOperationTimeoutError`는 보냈지만 결과를 모름, `ArcusPoolTimeoutError`는 풀 용량 신호다. 이 구분을 흐리는 변환이나 구분할 행동이 없는 새 예외는 낸다. (PR#7)
- 사용자 입력은 공개 진입점에서, Exchange를 만들기 전에 검증한다. 타입은 `TypeError`(int 자리의 `bool` 거절), 값은 `ValueError`다. 검증은 `_validation.py`에 모은다. (PR#7, PR#8, PR#22) (java-client 기준에서 가져옴: Operation 생성 전 맨 앞에서 검증)
- 검증·인코딩은 `prepare_*`에서 하고 generator 본문(`_set`, `_upsert`)에 두지 않는다. generator 본문은 첫 `next()`, 즉 풀에서 연결을 빌린 뒤에야 돌아서, 입력 오류가 연결 폐기로 이어진다. (`test_*_before_iteration` 테스트가 이 계약을 지킨다)
- 내부에서만 만드는 객체(풀, Routing)는 타입 검사를 반복하지 않는다. 이미 검증한 값을 다시 검증하지 않는다. (PR#7)
- `BaseException`은 정리하고 다시 던질 때만 잡는다. (PR#2)
- 실패는 예외로 알리고 None이나 sentinel로 돌려주지 않는다. 항목 상태로 정해지는 결과(1.6의 `False`/`None`)는 실패가 아니다. (팀 규칙)
- `try`는 처리하는 예외가 날 수 있는 줄만 감싸고 가장 좁은 타입을 잡는다. 호출자에게 의미가 더해질 때만 예외를 감싸거나 바꾸고, 문제를 이미 설명하는 기본 예외는 그대로 둔다. (팀 규칙)
- 소켓, 풀 연결, 락은 `with`나 `try/finally`로 모든 경로에서 놓는다. (팀 규칙)

### 1.6 Java 클라이언트·서버와의 호환
- 라우팅은 Java와 같아야 한다: MD5, 노드당 40회 × 4포인트(160), little-endian, `bisect_left`, 끝이면 0번, 포인트가 겹치면 주소가 사전순으로 앞선 노드. 바꾸면 기존 구현과 대조한 결과를 PR에 요구한다. (PR#3, PR#15)
- cache_list znode 이름은 `ip:port-hostname`이다. 잘못된 항목은 경고만 남기고 건너뛴다. (PR#2)
- 키 규칙(1~4000바이트, 공백·제어 문자 없음), flags 0, JSON 값, 16KB 초과 gzip, boolean 저장 형식을 바꾸면 Java 클라이언트와 같은 item을 읽고 쓸 수 있는지 묻는다. (PR#5) (java-client 기준에서 가져옴: flag 해석 변경은 기존 데이터·다른 transcoder 호환을 본다)
- 명령 문법, 값 범위, 응답 줄은 arcus-memcached 구현과 맞아야 한다. 새 범위나 응답이면 근거(서버 문서·코드)를 묻는다. 범위를 적은 주석이 코드와 같은지 본다(exptime은 64비트 signed). (PR#22)
- 명령마다 서버가 실제로 보내는 응답만 처리한다. 항목 상태로 정해지는 결과(`NOT_FOUND`, `TYPE_MISMATCH`, `EXISTS`)는 형제 명령처럼 `False`/`None`, 그 밖은 예외다. (java-client 기준에서 가져옴)

### 1.7 기존 코드와의 일관성
- 바뀐 코드마다 짝을 찾아 같은 방식인지 본다(`codebase.md` 4절). 가장 먼저 sync ↔ asyncio 짝이다: connection, pool, cluster, 그리고 PR#26 이후 `client.py` ↔ `aio.py`.
- 짝 중 한쪽만 바뀌었으면 의도인지 묻고, 같은 버그가 다른 쪽에도 있으면 함께 낸다. (java-client 기준에서 가져옴: 형제 클래스 불일치)
- 새 명령은 기존 명령의 모양을 따른다: `_command`의 `_xxx` generator + 검증하는 `prepare_xxx`, `client.py` 메서드는 `_encode_key` → `prepare_xxx` → `self._cluster.execute(key, exchange)`.
- 로그는 `arcus._logging.logger`. 캐시 값과 인증 정보를 로그에 남기지 않는다.
- 등급: 방식이 달라 동작도 달라지면(연결 누수, 다른 예외가 나감) 그 영향으로 매긴다. 동작은 같고 방식만 다르면 P3.

### 1.8 설계와 공개 표면
- 계층 경계: `_protocol`과 `_command`는 I/O가 없다. Exchange는 `Write`/`ReadLine`/`ReadExactly`를 yield하고 bytes를 받을 뿐 소켓, 시간, 스레드, asyncio를 만지지 않는다. 이 둘에 전송 코드가 들어오면 낸다. (팀 규칙)
- 전송 계층 동작: 클라이언트 생성에는 부작용이 없다(ZooKeeper 세션, 소켓 없음. 첫 요청이 시작한다). 노드가 cache list에서 빠지면 그 풀을 닫고 진행 중인 요청을 바로 abort한다(Java 클라이언트와 같다). `close()`는 새 요청을 거절하고 진행 중인 요청을 abort한다. (팀 규칙)
- 새 공개 이름은 `arcus/__init__.py`의 `__all__`에 있어야 하고, 공개 예외는 `Arcus` 접두어다. 내부 상태를 이유 없이 public으로 바꾸면 Q. (PR#18)
- 사용자가 고르는 값은 공개 enum으로 받는다(`Metric`, `Quant`). 시간·크기 옵션은 키워드 전용(`*`). 값 객체는 `@dataclass(frozen=True, slots=True)`. (PR#22)
- 테스트를 위해서만 있는 인자·Protocol·주입 callable은 낸다. 테스트는 import 위치에서 `mocker.patch`/`mocker.patch.object`로 바꾼다. 추상화(Protocol, ABC, factory, 주입 callable)는 운영에 필요하거나 실제 구현이 둘일 때만 둔다. (팀 규칙)
- 옛 arcus-python-client를 위한 호환 shim은 넣지 않는다. 호환 대상은 Java 클라이언트다. (팀 규칙)
- 새 설정·상태가 왜 필요한지 드러나지 않으면 Q. (PR#8)

### 1.9 주석, docstring, PR 제목 (P3)
- 주석은 무엇이 아니라 왜를 쓴다. 두 줄 이상 docstring은 여는 따옴표 다음 줄에서 요약을 시작하고, 한 줄 docstring은 한 줄로 둔다. (팀 규칙)
- PR·커밋 제목은 `PREFIX: Summary`다. PREFIX는 `FEATURE`, `INTERNAL`, `FIX` 같은 대문자 분류, Summary는 영어 명령형, 첫 글자 대문자, 마침표 없음. 어긋나면 P3로 한 줄 남긴다. (팀 규칙)

### 1.10 효율
- 요청마다 도는 경로(연결 읽기, 키 해시, 응답 파싱)에서 버퍼를 처음부터 다시 찾거나, 슬라이싱·변환을 반복하는 곳. (PR#3, PR#10)
- 지적하되 지금 고치라고 요구하지 않는다. 요청마다 응답 크기에 비례해 커지는 비용만 P2, 나머지는 P3.

### 1.11 테스트
- 새 명령은 `tests/support/response_cases.py`의 `CASES`에 응답 케이스(정상, 항목 상태별 False/None, 잘못된 줄)를 넣었는지 본다. 이 목록이 protocol·sync·asyncio 세 경로를 함께 돈다. 클라이언트 테스트를 넣었으면 `fake_server.py`도 그 명령을 알아야 한다.
- 동작 변경에는 이전 동작에서 실패하는 테스트가 있는지 본다. 동시성 테스트는 제한 시간 있는 대기를 쓰고 맨 `sleep`에 의존하지 않는다. 기다리는 시간 상수에는 이유가 드러나는 이름을 붙인다. 테스트는 `# Given:` / `# When:` / `# Then:` 주석으로 나누고, 운영 코드처럼 타입을 표기한다. (팀 규칙)
- 테스트가 없으면 P3로 한 줄 남긴다. 사람 리뷰어는 테스트를 요구한 적이 없다.

---

## 2. 지적하지 않는 것

- **CI가 잡는 것.** ruff: 포매팅, import 순서(`I`), 미사용 이름(`F`), 타입 표기 누락(`ANN`), 옛 문법(`UP`), 가변 기본 인자·`raise ... from` 누락 등(`B`), 단순화(`SIM`), async 안의 일부 블로킹 호출(`ASYNC`), `RUF`. ty: 타입 오류(경고도 실패). 이 규칙에 걸리는 것은 CI가 실패시키므로 따로 내지 않는다.
- **이름·배치 취향.** public 메서드 위치(PR#10), 플래그 초기값 표현(PR#2)처럼 한 번 언급하고 넘어간 것. 기존 짝 이름과 어긋나거나 헷갈릴 때만 P3.
- **코드 제안을 그대로 쓰라는 요구.** 작성자는 리뷰 제안을 자기 방식으로 반영해 왔다(PR#2, PR#10, PR#15, PR#17). 문제와 영향만 쓰고 고치는 방식은 맡긴다.
- **호출 계약이 막는 방어 코드.** 차집합으로 중복이 없는 곳에 `discard`/`get`을 넣으라고 하지 않는다(PR#3). 정상 서버가 보낼 수 없는 응답의 검사도 요구하지 않는다. (java-client 기준에서 가져옴)
- **공개 API 호환.** 출시 전이라 이름·시그니처 변경, 옛 arcus-python-client 호환은 보지 않는다.
- **알려진 문제와 범위 밖.** 디코드 실패 시 연결 폐기(PR#10), FIN/RST 없이 끊긴 유휴 연결(PR#28), asyncio 쪽에 아직 없는 `execute_all`·클라이언트, `TODO`. 이번 PR이 키울 때만 낸다.
- **근거 없는 일반론.** 확인하지 못한 경쟁·누수는 단정하지 않고 `[확인 필요]`로 보낸다.

## 3. 등급 (이 프로젝트의 예)

등급의 정의는 `core-output.md`에 있다.

| 등급 | 예 |
|---|---|
| P1 | Exchange가 `END` 전에 return해 다음 요청이 남은 줄을 읽음, 실패한 연결을 풀에 반납, 풀 슬롯·`_size` 누수, kazoo 스레드에서 asyncio 객체를 직접 변경, fork한 자식이 부모 ZooKeeper 세션·소켓을 닫음, 라우팅이 Java와 달라짐 |
| P2 | sync만 고치고 asyncio 짝을 빠뜨림, 검증이 generator 본문에 있어 연결 폐기로 이어짐, 파싱 실패가 `ValueError`로 샘, 예외 의미 혼동(타임아웃 ↔ 연결 오류), 취소 시 정리 누락, 끝나지 않을 수 있는 await, 이벤트 루프에서 블로킹 호출, CI 실패 |
| Q | `BaseException`을 잡는 이유, 내부 상태를 public으로, 새 옵션·예외·설정의 필요성, 닫는 이유를 넘기는 설계, 새 값 범위의 근거 |
| P3 | 테스트·`CASES` 케이스 없음, 기존 짝과 다른 방식(검증 위치, 예외 변환, 로그), 반복 계산, 헷갈리는 이름, 코드와 다른 주석 |

## 4. 대표 사례

- **PR#7 sync/pool.py:** `close(reason)`가 대기자에게 `copy.copy(reason)`을 던진다. 리뷰어가 이유를 묻자 노드 제거와 클라이언트 종료를 구분하기 위해서라고 답했다. 이어서 원인을 붙인 예외는 복사 때 원인이 빠진다는 지적에 주석으로 계약을 남겼다. → 닫는 이유는 예외로 구분하고, 나눠 주는 예외는 메시지만 가진 새 예외로 만든다.
- **PR#3, PR#15 locator.py:** `update(to_attach, to_delete)`로 ring만 먼저 바뀌면 풀이 없는 노드로 라우팅될 수 있었다. locator를 불변으로 바꾸고 `Routing`째 교체했다. 노드 51개, 키 20만 개로 기존 구현과 결과가 같음을 확인했다. → 함께 바뀌는 상태는 한 객체로 묶어 바꾸고, 라우팅 변경은 Java와 대조한다.
- **PR#13, PR#21 cluster.py:** fork 뒤 자식이 부모의 연결로 요청하면 부모가 다른 키의 값을 에러 없이 받았다(redis-py로 재현). sync는 pid로 재초기화하고, asyncio는 첫 루프에 묶여 다른 루프를 `acquire()` 전에 거절한다. → 부모 상태는 닫지 말고 버리며, 검사는 연결에 쓰기 전에 한다.
- **PR#10 sync/connection.py:** `execute()`의 `except BaseException`이 `END`까지 읽은 뒤의 디코드 실패까지 연결 폐기로 처리한다(기록용, 미수정). → 연결을 버릴지는 응답을 끝까지 읽었는지로 정한다. 이 문제 자체는 다시 내지 않는다.
- **PR#17 aio/connection.py:** `close()`의 `await writer.wait_closed()`는 쓰기 버퍼가 남고 상대가 읽지 않으면 끝나지 않는다. 버퍼가 남으면 `abort()`하도록 고쳤다. asyncio 연결은 응답 뒤 남은 바이트를 검사하지 않는다고 PR 본문에 적었다. → asyncio 쪽은 응답 경계와 종료 대기를 sync보다 더 엄하게 본다.
- **PR#22 vector.py:** `quant: str`, `maxcount`, 명령 모듈 안의 검증 함수, "4-byte signed exptime" 주석. enum 공개, `max_count`, `_validation.py`로 모으기, 주석 수정이 모두 반영됐다. → 사용자가 고르는 값은 공개 enum, 검증은 한 곳, 범위 주석은 코드와 같게.
- **PR#2 discovery.py:** `except BaseException`의 이유를 묻자 시작 실패나 중단에도 ZooKeeper 클라이언트를 정리하고 다시 던지려는 것이라고 답했고, 그대로 통과했다. → `BaseException`은 정리 후 재전달일 때만 허용한다.
