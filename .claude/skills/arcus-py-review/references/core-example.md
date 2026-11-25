<!-- generated from core/example.md by scripts/sync_core.sh; edit core/ instead -->

# 리뷰 출력 예시

**형식과 말투만 따른다.** 지적의 개수, 등급 분포, 내용은 PR마다 다르다. 예시에 맞추려고 지적을 만들거나 빼지 않는다.
예시와 `core-output.md`의 규칙이 다르면 규칙을 따른다. 예시의 코드는 ARCUS memcached의 것이지만, 다른 프로젝트에서도 형식만 본다.

예시 1은 지적이 여러 개인 경우, 예시 2는 문제가 없는 경우, 예시 3은 사소한 것만 있는 경우다.

---

## 예시 1: 지적이 여러 개인 경우

PR#772(map upsert 추가) 첫 버전을 봇이 리뷰했다면 어떻게 나왔을지 가정해서 썼다. 내용은 실제 리뷰에서 결론이 난 것들이다.

### 인라인 코멘트

#### engines/default/coll_map.c:709
~~~
🔴 **[P1]**
overflow 검사가 insert와 교체를 구분하지 않습니다.

가득 찬 map에서는 개수가 늘지 않는 upsert 교체도 `ENGINE_EOVERFLOW`로 실패합니다.

insert인지 교체인지는 `do_map_elem_link()`에서 정해집니다. 검사를 그 안의 insert 경로로 옮기는 것을 제안합니다.

> 검사 위치가 바뀌어도 응답 종류는 같아서 client 호환에는 영향이 없습니다.
~~~

#### engines/default/coll_map.c:807
~~~
**[P3]**
`replaced`는 교체 경로에서만 값이 정해집니다.

`*created = false;` 옆에서 `*replaced = false;`도 초기화하면 모든 경로에서 값이 정해집니다.
~~~

#### memcached.c:1846
~~~
**[Q]**
operation type을 `OPERATION_MOP_UPSERT`로 따로 둔 이유가 있나요?

`OPERATION_MOP_INSERT`에 upsert 플래그를 두는 방식도 가능합니다. 정한 방식을 bop upsert에도 맞출 수 있습니다.
~~~

#### memcached.c:11829
~~~
**[P3]** 기존 코드 문제, 별도 PR 권장
이 assert는 항상 통과합니다.

`||` 오른쪽의 `(int)OPERATION_MOP_UPDATE`가 0이 아닌 상수이기 때문입니다.

`cmd == (int)OPERATION_MOP_UPDATE`로 고치는 수정을 별도 PR로 먼저 반영하는 것을 제안합니다.
~~~

#### memcached.c:11881
~~~
**[P3]** 기존 코드 문제, 별도 PR 권장
`settings.detail_enabled`일 때 이 분기는 prefix 통계를 기록하지 않습니다.

위 assert와 같은 별도 PR에서 고칠 수 있습니다.
~~~

#### memcached.c:11877
~~~
**[P3]**
`} else /* OPERATION_MOP_UPDATE */ {` => `} else { /* OPERATION_MOP_UPDATE */`
~~~

### 요약 코멘트

~~~
overflow 검사가 교체 요청에도 적용되어, 가득 찬 map에서 upsert 교체가 실패합니다.
교체 경로의 공간 계산과 해제는 맞습니다. (P1 1 · P2 1 · Q 1 · P3 4)

### 검토 요청
- 🔴 **[P1]** coll_map.c:709 — overflow 검사가 교체 요청에도 적용됨. `do_map_elem_link()` 안으로 옮기기
- **[P2]** 새 명령의 테스트가 `t/`에 없습니다. 아래 경우를 확인하는 테스트가 필요합니다.
  - 새 field 추가
  - 기존 field 교체
  - 가득 찬 map에서의 교체
- **[Q]** memcached.c:1846 — operation type을 나눈 이유
- **[확인 필요]** replication에서 upsert 교체를 적용하는 경로는 봇이 볼 수 없어 확인하지 못했습니다.

### 그 밖의 제안
- **[P3]** coll_map.c:807 — `*replaced = false`를 `*created`와 함께 초기화
- **[P3]** memcached.c:11829 — 항상 참인 assert (기존 코드 문제, 별도 PR 권장)
- **[P3]** memcached.c:11881 — prefix 통계 누락 (기존 코드 문제, 별도 PR 권장)
- **[P3]** memcached.c:11877 — else 주석 위치
~~~

---

## 예시 2: 문제가 없는 경우

### 인라인 코멘트
(인라인 코멘트 없음)

### 요약 코멘트

~~~
확인한 범위에서 문제를 찾지 못했습니다. 기존 `do_btree_elem_update()`와 같은 구조이고, 교체에 실패했을 때 새 element를 해제하는 경로도 맞습니다. (P1 0 · P2 0 · Q 0 · P3 0)
~~~

---

## 예시 3: 사소한 것만 있는 경우

### 인라인 코멘트

#### engines/default/coll_set.c:412
~~~
**[P3]**
함수 사이에 빈 줄이 두 개 있습니다. 이 파일의 다른 함수 사이에는 한 개입니다.
~~~

### 요약 코멘트

~~~
동작에 영향이 있는 문제는 찾지 못했습니다. (P1 0 · P2 0 · Q 0 · P3 1)

### 검토 요청
없습니다.

### 그 밖의 제안
- **[P3]** coll_set.c:412 — 함수 사이 빈 줄
~~~
