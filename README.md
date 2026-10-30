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
cp .env.example .env
# Вкажіть host, порти gateway, TLS server name та шлях до CA PEM.
cargo run --locked
# Оптимізований застосунок:
cargo run --locked --release
```

Апка читає конфігурацію шлюзу з `.env` (або змінних середовища процесу) і
підключається під час запуску. Задайте `GATEWAY_HOST`,
`GATEWAY_PORT` (один ingress-порт для QUIC, TLS і TCP), `GATEWAY_TLS_SERVER_NAME`,
`GATEWAY_CA_FILE` (довірений CA PEM) і
`GATEWAY_CONTROL_PORT`. Тайм-аути підключення та повторів теж задаються в
`.env.example`. `GATEWAY_PING_INTERVAL_MS` задає періодичність ping;
`GATEWAY_MISSED_PONGS` і `GATEWAY_RECOVERY_ATTEMPTS` задають поріг пропущених
pong та кількість спроб відновити поточний транспорт перед повним перепідключенням.
`GATEWAY_UP_RETURN_SEC` задає інтервал моніторингу стану транспортів і спроб
повернутися на бажаний транспорт (типово 30 секунд).
RPC-підключення тричі пробує QUIC, потім тричі TLS і тричі TCP.
Усі RPC-вкладки використовують одну сесію; окремий канал Control Plane
підключається тим самим транспортом, що й активна Gateway-сесія.
Якщо `.env` або обов’язкові значення відсутні чи некоректні, апка відкриється
з повідомленням про помилку конфігурації замість аварійного завершення;
після виправлення `.env` перезапустіть її.

У RustRover працює наявна Cargo-конфігурація **Run**.
Linux потребує графічної сесії X11 або Wayland. Типові пакети Debian/Ubuntu для
збирання: `build-essential`, `pkg-config`, `cmake`, `libssl-dev`,
`libxkbcommon-dev`, `libwayland-dev`, `libx11-dev`, `libxi-dev` та Mesa/OpenGL
для запуску. На macOS потрібні Xcode Command Line Tools; на Windows — MSVC
Build Tools. Інтеграційні перевірки в цьому середовищі виконуються на Linux;
підтримка окремих транспортів залежить також від upstream-бібліотеки й ОС.

## Можливості клієнта

| Екран | Що демонструється |
| --- | --- |
| Gateway connection | Автоматичне підключення зі значень `.env`; QUIC → TLS → TCP, по три спроби на транспорт; спільна RPC-сесія для всіх вкладок |
| Request | `list_methods` із Control Plane; вибір лише оголошених backend-методів і підстановка кожного зразка `method_details.example_request.params`, перегляд `params_schema`, структуровані object/array params, відсутні params, `notify`, типізоване `request<P, R>` із serde та `request_without_params<R>` |
| Batch | Змішані виклики/сповіщення, автоматичні ID, кореляція результатів у порядку вхідних викликів, окремі remote errors; notification-only batch |
| Parallel calls | Клонування клієнта зі спільною сесією та ID; 1–64 конкурентних запити, окремий результат/помилка кожного |
| Policy & liveness | Автоматичний ping за `GATEWAY_PING_INTERVAL_MS`; поріг missed pong і кількість повторів restore задаються в `.env` та змінюються в Runtime policy; після вичерпання restore виконується Gateway fallback reconnect |
| Transport controls | Транспортний ping/pong без RPC, restore, reconnect із повтором gateway fallback; панель Connection читає `list_transports` і надсилає `set_transport_enabled` для профілів QUIC/TLS/TCP |
| Backends | `list_backends`, реєстрація backend через `add_backend`, увімкнення/вимкнення маршрутизації через `set_backend_enabled`; список автоматично оновлюється |
| Running operations | Скасування конкретної операції або всіх, включно з паралельними дочірніми запитами |
| Metrics | Справжній `ClientMetricsSnapshot`: активні запити, черга, RPC calls/errors, timeouts, cancellations, recovery attempts, missed pongs; застосована конфігурація |
| Event log | Події `tracing` бібліотеки, liveness/recovery, фільтр, рівні, копіювання; опційний DEBUG/TRACE |
| Мапа | Граф Gateway → in-process Balancer → backend-и; ingress endpoints, circuit breakers та зайнятість connection pools через Control Plane |

### Мапа екосистеми Gateway

Каталог методів допускає відсутній або `null` приклад запиту. Такий метод
залишається доступним для ручного введення параметрів. Некоректний приклад чи
форма схеми показуються попередженням біля вибраного методу, не блокуючи інші
методи. Помилки структури каталогу (зокрема дублікати назв) залишаються помилками.

The method catalog accepts missing or `null` request examples, leaving those
methods available for manual parameter entry. Malformed examples or schema
shapes produce a warning for the selected method without blocking other methods.
Structural catalog errors, including duplicate method names, remain errors.

Вкладка **Мапа** автоматично опитує Control Plane gateway-а за портом
`GATEWAY_CONTROL_PORT`. Це окремий канал до того самого gateway, незалежний
від спільної RPC-сесії. Наразі Control Plane примусово використовує TCP
незалежно від транспорту основної Gateway-сесії; надсилається команда
`{"action":"list_components"}`, а не JSON-RPC envelope. Інтервал оновлення
можна змінювати у вкладці; host і порт беруться зі спільної конфігурації `.env`.

Мапа показує версію/PID/uptime gateway-а, ingress listeners, control/metrics
адреси, версію та режим виконання balancer-а, всі налаштовані backend-и,
їхні host/transport/port, pool size та idle/empty/busy slots. Circuit states
позначені кольорами: `closed` — бірюзовий, `open` — червоний, `half_open` —
жовтий; також показані failures, retry delay або probe-in-flight відповідно.
Масштаб і прокрутка дозволяють переглянути великий граф; наведення на вузол
показує повний текст адрес та станів. Оновлення автоматичне, також під час
перегляду інших вкладок. TCP-з'єднання Control Plane повторно використовує
сесію для наступних запитів. Новий запит починається через заданий інтервал після завершення
попереднього, без накладання запитів. Інтервал можна змінювати під час роботи.
При обриві чи тайм-ауті остання мапа зберігається з повідомленням про помилку
та віком знімка, а наступне оновлення повторно підключається до Control Plane.
Підключення мапи запускається разом із застосунком; її окремий канал
перепідключається на наступному циклі оновлення. Опитування починається після
успішного підключення основної Gateway-сесії.

Панель **Connection** отримує стан транспортних профілів через Control Plane.
Перемикач надсилає `set_transport_enabled`; вимкнення профілю закриває його
активні сесії та відхиляє нові. Повторне ввімкнення дозволяє нові сесії, але
не відновлює ті, що були закриті. Вимкнення активного профілю запускає
перепідключення з порядком QUIC → TLS → TCP.
Кожні `GATEWAY_UP_RETURN_SEC` секунд TCP-сесія пробує перейти на QUIC,
а за невдачі — TLS; TLS-сесія пробує лише QUIC. Спроба створює новий транспорт,
поки попередня сесія обслуговує запити; стара закривається лише після успіху.
Невдалі спроби зберігають поточну сесію й повторюються через той самий інтервал.
Під час fallback вимкнені профілі пропускаються.

Пункт **Backends** у верхньому меню відкриває список backend-ів із ID, host/IP,
портами, enabled, active requests і станом пулів. Форма реєстрації приймає ID,
host, JSON-об'єкт портів (наприклад `{"tcp":18081}`) та pool size. Реєстрація
оновлює конфігурацію балансування, але не запускає backend-процес. **Disable**
припиняє маршрутизацію нових запитів до backend-а; це не зупиняє сам процес.
Його поточні запити та сесії завершуються за правилами Control Plane. Список
оновлюється кожні п'ять секунд, поки вікно відкрите.

Це **неатомарний знімок конфігурації та локального стану пулів**, не активна
перевірка здоров'я. Closed circuit або наявний connection не доводить, що
backend-процес працює. Стрілки — налаштовані зв'язки, не виміряний трафік.
Вік знімка й endpoint джерела показані явно. Control Plane не має
автентифікації: використовуйте лише приватну мережу або захищений тунель,
не відкривайте його публічно. Відповідь обмежена 4 MiB.

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

Бібліотека містить 30 транспортних реалізацій для інтеграційних перевірок.
GUI-клієнт навмисно використовує тільки gateway ingress з fallback QUIC → TLS
→ TCP; інші варіанти з таблиці не є доступними опціями підключення в апці.

| Група | Варіанти | Тестовий демосервер |
| --- | --- | --- |
| TCP/HTTP | TCP, TLS/TCP, HTTP/1.1 | Так |
| Datagram | reliable UDP, RTP/RTCP, RUDP | Так |
| QUIC | QUIC, HTTP/3 | Так |
| WebSocket | WS/TCP, WS/HTTP1 Upgrade, WSS, WS/UDP, WS/RTP, WS/RUDP, WS/QUIC, WS/HTTP3 | Так |
| Kernel-specific | UDP-Lite, DCCP, SCTP та їх WS-варіанти | Зовнішній сервер; Linux і підтримка протоколу ядром |
| SRT | native SRT та WS/SRT | Зовнішній сервер; feature `native-srt` |
| IoT | MQTT, CoAP, AMQP та їх WS-варіанти | Зовнішній peer; для MQTT/AMQP потрібні broker та RPC consumer |

Інші реалізації транспорту лишаються в бібліотеці та інтеграційних тестах, але
не доступні для вибору в GUI. Повний огляд специфічних параметрів MQTT/AMQP,
SRT і kernel-specific transport-ів наведено в документації upstream:
[gateway-iot](https://github.com/vsvictor/JSON-RPC-2/blob/0a5cee9c7cd5e2e1ecb4c123c3f99bceeaf6044f/docs/BROKER-INTEGRATION.md).

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
src/ecosystem.rs    Control Plane commands, typed inventory, bounded transport reads
src/ecosystem_map.rs graphical ecosystem map, discovery lifecycle
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
Тести Control Plane перевіряють точні байти команди, фрагментовану відповідь,
усі circuit states, порожній inventory, відмови, невалідні/незавершені та
завеликі відповіді й тайм-аут.

### Важливі upstream-деталі

- JSON-RPC-2 зафіксована на `1d519f47d5cc44d1986fd09c738a9b4609536e07`.
  Оновлена бібліотека додає `rpc.list_methods` з описами JSON Schema,
  aliases і `ServerPlugin`, а також окремі `json-rpc-2-backend` та
  `json-rpc-2-gateway` crates. Desktop-клієнт використовує оновлені клієнтські
  API та Control Plane; серверні crates не додаються до застосунку, оскільки
  він не збирає backend-плагіни.
  Workspace patches не успадковуються downstream-проєктами, тому
  `Cargo.toml` явно фіксує ті ж `h3`/`h3-quinn` git patches для extended CONNECT.
- У цій ревізії WS/QUIC та WS/HTTP3 під час reconnect ділять QUIC endpoint між
  старою і новою сесіями, тоді як close очікує endpoint-wide idleness.
  `EndpointSession` створює новий незалежний endpoint перед закриттям старого,
  зберігаючи native framing, TLS verification і cancellation hooks.
  Інтеграційні тести перевіряють ping і RPC після такого reconnect.

## Дані та обмеження

Конфігурація читається з `.env`, а довірений CA — з указаного в ньому PEM-файлу;
застосунок їх не змінює. Лог обмежений 500 подіями, відображення одного
запису/результату — 32 KiB; обрізання й переповнення черги позначаються явно.
DEBUG/TRACE можуть містити credentials, params і персональні дані — вмикайте їх
свідомо, не публікуйте чутливий журнал. Копіювання в clipboard здійснюється
тільки кнопкою.

«Session open» означає створену клієнтську сесію, а не постійну гарантію
досяжності peer. Для стану мережі використовуйте ping, missed pongs та події
відновлення. Це демонстрація публічних RPC API та транспортних налаштувань,
а не редактор власних wire codecs чи серверних extensions.
