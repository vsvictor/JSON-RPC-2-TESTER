# JSON-RPC-2 Desktop Client Lab

Нативний desktop-аналог [JsonRPCClientKMPExample](https://github.com/vsvictor/JsonRPCClientKMPExample):
**Rust + egui/eframe**, пряме використання Rust-клієнта
[JSON-RPC-2](https://github.com/vsvictor/JSON-RPC-2), без JVM, WebView або FFI.
Мережеві операції виконуються у Tokio, окремо від UI.

## Запуск

Потрібні актуальний stable Rust з підтримкою edition 2024, Git та системний
C/C++ toolchain. Залежності бібліотеки зафіксовані в `Cargo.lock`;
використовуйте `--locked` для відтворюваного збирання.

```sh
cargo run --locked
# Оптимізований застосунок:
cargo run --locked --release
```

У RustRover працює наявна Cargo-конфігурація **Run**.
Linux потребує графічної сесії X11 або Wayland. Типові пакети Debian/Ubuntu для
збирання: `build-essential`, `pkg-config`, `cmake`, `libssl-dev`,
`libxkbcommon-dev`, `libwayland-dev`, `libx11-dev`, `libxi-dev` та Mesa/OpenGL
для запуску. На macOS потрібні Xcode Command Line Tools; на Windows — MSVC
Build Tools. Інтеграційні перевірки в цьому середовищі виконуються на Linux;
підтримка окремих транспортів залежить також від upstream-бібліотеки й ОС.

### Перший сценарій без зовнішніх сервісів

1. Натисніть **Start demo server**. Сервер слухає тільки `127.0.0.1`,
   вибирає вільні порти та автоматично підставляє TCP endpoint.
2. Натисніть **Connect**, потім **Call**. Метод `echo` поверне ваш JSON.
3. Спробуйте пресети, **Notify**, **Typed greet**, **Parallel calls**, вкладки
   **Batch**, **Policy & liveness**, **Metrics** та **Event log**.

**Use demo endpoint** дозволяє вибрати будь-який із 16 локальних транспортів.
Для TLS/QUIC застосунок генерує тимчасовий сертифікат `localhost` і явно
довіряє йому тільки в налаштуваннях цього клієнта. Ключ не записується на диск,
системне сховище довіри не змінюється, перевірка імені сервера не вимикається.
Сертифікат і порти змінюються після перезапуску демосервера.

## Можливості клієнта

| Екран | Що демонструється |
| --- | --- |
| Connection | Вибір транспорту, DNS/IP/IPv6, порт, тайм-аут підключення, TLS server name, CA PEM з тексту або файлу, WebSocket path |
| Request | `request_value`, структуровані object/array params, відсутні params, `notify`, типізоване `request<P, R>` із serde та `request_without_params<R>` |
| Batch | Змішані виклики/сповіщення, автоматичні ID, кореляція результатів у порядку вхідних викликів, окремі remote errors; notification-only batch |
| Parallel calls | Клонування клієнта зі спільною сесією та ID; 1–64 конкурентних запити, окремий результат/помилка кожного |
| Policy & liveness | Тайм-аут запитів, ping interval, missed pong threshold, restore-before-reconnect, cooldown/backoff/jitter/budget, черга та in-flight limit |
| Transport controls | Транспортний ping/pong без RPC, restore, reconnect |
| Running operations | Скасування конкретної операції або всіх, включно з паралельними дочірніми запитами |
| Metrics | Справжній `ClientMetricsSnapshot`: активні запити, черга, RPC calls/errors, timeouts, cancellations, recovery attempts, missed pongs; застосована конфігурація |
| Event log | Події `tracing` бібліотеки, liveness/recovery, фільтр, рівні, копіювання; опційний DEBUG/TRACE |

Форма **Policy** застосовується при підключенні. **Apply policy** оновлює
відкриту сесію. Upstream дозволяє міняти місткість черги лише при створенні
сесії, а in-flight limit — тільки збільшувати. UI та перевірки перед
`set_config` зберігають ці інваріанти; для зменшення спершу від'єднайтеся.
Автоматичне відновлення працює при ввімкнених періодичних ping і **не повторює
RPC-виклики**.

**Cancel** відкидає локальний future, а бібліотека прибирає pending request
і передає скасування транспортам, що це підтримують. Це не команда скасування
методу на сервері й не rollback його побічних ефектів.

### Транспорти

У каталозі 30 варіантів; недоступні в поточному build/ОС позначені явно.

| Група | Варіанти | Локальний демосервер |
| --- | --- | --- |
| TCP/HTTP | TCP, TLS/TCP, HTTP/1.1 | Так |
| Datagram | reliable UDP, RTP/RTCP, RUDP | Так |
| QUIC | QUIC, HTTP/3 | Так |
| WebSocket | WS/TCP, WS/HTTP1 Upgrade, WSS, WS/UDP, WS/RTP, WS/RUDP, WS/QUIC, WS/HTTP3 | Так |
| Kernel-specific | UDP-Lite, DCCP, SCTP та їх WS-варіанти | Зовнішній сервер; Linux і підтримка протоколу ядром |
| SRT | native SRT та WS/SRT | Зовнішній сервер; feature `native-srt` |
| IoT | MQTT, CoAP, AMQP та їх WS-варіанти | Зовнішній peer; для MQTT/AMQP потрібні broker та RPC consumer |

Транспортний selector має стартові порти-підказки, які можна змінювати.
Під час вибору локального endpoint використовуються фактичні динамічні порти,
а не ці підказки. Для зовнішнього сервера введіть його реальні параметри.

MQTT/AMQP налаштовуються через **broker URL**, **request topic/queue** та
окремі operation/response timeouts. MQTT підтримує custom CA PEM; AMQP
використовує системні корені довіри. Звичайний broker сам не виконує RPC:
потрібен відповідний consumer, наприклад
[gateway-iot](https://github.com/vsvictor/JSON-RPC-2/blob/0a5cee9c7cd5e2e1ecb4c123c3f99bceeaf6044f/docs/BROKER-INTEGRATION.md).

UDP-Lite має send/receive checksum coverage (`0` — весь пакет, інакше `>= 8`).
Для TCP-сімейства доступні TCP_NODELAY, BBR і Fast Open; непідтримувані
системою опції повертають помилку, а не ігноруються.

WS/TCP у цій бібліотеці — окрема raw WebSocket-сесія **без HTTP Upgrade**.
Для звичайного `ws://.../ws` використовуйте **ws-http1**; для `wss://` —
**ws-tls**. WS поверх datagram/IoT/SRT — приватні bindings бібліотеки,
не універсальні браузерні WebSocket endpoints. Дивіться upstream
[матрицю сумісності](https://github.com/vsvictor/JSON-RPC-2/blob/0a5cee9c7cd5e2e1ecb4c123c3f99bceeaf6044f/docs/CONFORMANCE.md).

### Native SRT

SRT винесений в опційну feature, щоб стандартний GUI не вимагав libsrt.
Встановіть **libsrt >= 1.5 з encryption**, pkg-config та libclang:

```sh
# Debian/Ubuntu:
sudo apt install libsrt-openssl-dev pkg-config libclang-dev
# macOS:
brew install srt pkg-config llvm

cargo run --locked --features native-srt
```

Для нестандартного встановлення задайте `PKG_CONFIG_PATH`, за потреби
`LIBCLANG_PATH`, а при запуску — шлях до shared libraries.
SRT passphrase має містити 10–79 **байтів** і збігатися з серверним.
Немає нешифрованого fallback чи підміни SRT власним протоколом.
Платформні інструкції:
[upstream SRT](https://github.com/vsvictor/JSON-RPC-2/blob/0a5cee9c7cd5e2e1ecb4c123c3f99bceeaf6044f/docs/SRT.md).

## Демонстраційні методи

| Метод | Params | Результат |
| --- | --- | --- |
| `echo` | Object або array | Вхідні params |
| `sum` | `[1, 2, 3]` | `6.0` |
| `greet` | `{"name":"Desktop"}` | Типізований object з `message` і `source` |
| `info` | Без params | Інформація про демосервер |
| `slow` | `{"ms":10000}` | Відкладений результат; затримка до 60 секунд |
| `fail` | Неважливо | Remote error `-32001` з `data` |
| Невідомий метод | Неважливо | Стандартна помилка `-32601` |

Щоб побачити timeout, задайте **Request timeout** менший за `slow.ms`,
застосуйте політику і викличте `slow`. Для скасування задайте більший timeout,
запустіть `slow` і натисніть **Cancel**. Лічильники видно у **Metrics**.

## Структура й перевірки

```text
src/main.rs         desktop entry point
src/ui.rs           egui, асинхронні операції, bounded event log
src/connection.rs   каталог, валідація, transports, TLS, runtime policy
src/requests.rs     params/batch parsing, parallel calls, error presentation
src/demo.rs         localhost sandbox, ephemeral TLS identity
tests/showcase.rs   реальні мережеві інтеграційні сценарії
```

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
# Якщо встановлено native libsrt:
cargo test --locked --all-targets --features native-srt
```

Інтеграційні сценарії покривають усі 16 локальних транспортів: typed/untyped
RPC, notifications, mixed/notification-only batch, parallel calls, remote
errors, ping/restore/reconnect/close. Окремо перевіряються timeout,
cancellation cleanup, live policy та відмова TLS при неправильному імені
сервера чи недовіреному сертифікаті. Для запуску тестів потрібні дозволені
localhost TCP/UDP sockets, але не GUI, broker або зовнішній RPC-сервер.
SRT/IoT/kernel-specific з'єднання потребують окремого середовища; стандартні
тести не стверджують їх мережеву сумісність.

### Важливі upstream-деталі

- JSON-RPC-2 зафіксована на `0a5cee9c7cd5e2e1ecb4c123c3f99bceeaf6044f`.
  Її workspace patches не успадковуються downstream-проєктами, тому
  `Cargo.toml` явно фіксує ті ж `h3`/`h3-quinn` git patches для extended CONNECT.
- У цій ревізії WS/QUIC та WS/HTTP3 під час reconnect ділять QUIC endpoint між
  старою і новою сесіями, тоді як close очікує endpoint-wide idleness.
  `EndpointSession` створює новий незалежний endpoint перед закриттям старого,
  зберігаючи native framing, TLS verification і cancellation hooks.
  Інтеграційні тести перевіряють ping і RPC після такого reconnect.

## Дані та обмеження

Налаштування, PEM, паролі та журнал не зберігаються на диск. Лог обмежений
500 подіями, відображення одного запису/результату — 32 KiB; обрізання й
переповнення черги позначаються явно. Повні результати більших відповідей
потребують окремого клієнтського коду. DEBUG/TRACE можуть містити credentials,
params і персональні дані — вмикайте їх свідомо, не публікуйте чутливий журнал.
Копіювання результату/журналу в clipboard здійснюється тільки кнопкою.

«Session open» означає створену клієнтську сесію, а не постійну гарантію
досяжності peer. Для стану мережі використовуйте ping, missed pongs та події
відновлення. Це демонстрація публічних RPC API та транспортних налаштувань,
а не редактор власних wire codecs чи серверних extensions.
