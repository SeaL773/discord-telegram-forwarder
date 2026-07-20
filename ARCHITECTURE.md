# discord-tg-forwarder 架构设计

> Discord → Telegram 消息转发器。消费 [discord-message-bridge](https://github.com/SeaL773/discord-message-bridge) 的本地事件流,按可配置规则路由到指定 Telegram 群组/频道。
>
> 状态: **M0-M4 已实现并通过自动化验证**
> 日期: 2026-07-19

---

## 1. 定位与边界

```
[已完成]                                  [本项目]                        [未来项目]
Discord Desktop                     discord-tg-forwarder              LLM cron 分析器
+ Vencord collector  ──NDJSON──►  ◄──WS/REST── 消费事件               (直接读 Bridge
        │                              │                               REST API,与本
        ▼                              ▼                               项目无关)
discord-message-bridge            Telegram Bot API
(127.0.0.1:17891)                 (指定群组/频道)
```

**本项目只做三件事**:
1. 从 Bridge 稳定地消费事件流(不丢、不重)
2. 按规则决定"哪个 Discord 频道的消息 → 哪个 Telegram chat"
3. 格式化并发送到 Telegram(含媒体,含限流处理)

**明确不做**:
- 消息采集(Bridge 负责)
- LLM 分析(独立项目,直接消费 Bridge REST)
- DC↔TG 消息双向同步(单向转发)
- 编辑消息时修改已发的 TG 消息(所有事件都作为**独立通知**发送,无需维护消息 ID 映射库 —— 这是一个刻意的简化决策)

---

## 2. 已确认的技术决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 语言 | **Python 3.11+** | 与 monitor suite 生态一致(loguru / httpx),复用现有模式 |
| 部署 | **WSL Docker 容器** | 与其他 monitor 容器统一管理 |
| TG 发送 | **Bot API** | 简单稳定,bot 拉进目标群即可,与现有 monitor 推送方式一致 |
| 事件范围 | **CREATED / EDITED / DELETED / GHOST_PINGED 全部转发** | 每种事件作为独立通知,不维护消息映射 |
| 媒体 | **尝试转发媒体,失败降级为链接** | Discord CDN 链接有过期时间,能转尽转 |
| 投递语义 | **at-least-once + cursor 去重** | 宁可极小概率重复,不可丢消息 |

---

## 3. 总体架构

```
┌─────────────────────────── Windows 宿主 ───────────────────────────┐
│  Discord Desktop (Vencord collector)                               │
│      └─► %APPDATA%\Vencord\MessageLoggerData\message-events.ndjson │
│              └─► discord-message-bridge  (REST + WS, port 17891)   │
└───────────────────────────────┬────────────────────────────────────┘
                                │  ← Windows/WSL 网络边界 (见 §5)
┌────────────────────── WSL2 Docker 容器 ────────────────────────────┐
│  discord-tg-forwarder                                              │
│                                                                    │
│  ┌──────────────┐   ┌──────────┐   ┌───────────┐   ┌────────────┐  │
│  │ BridgeClient │──►│  Router  │──►│ Formatter │──►│  TgSender  │  │
│  │ WS+REST replay│   │ 规则引擎 │   │ +Media    │   │ 队列+限流  │  │
│  └──────┬───────┘   └────┬─────┘   └───────────┘   └─────┬──────┘  │
│         │                │                               │         │
│         ▼                ▼                               ▼         │
│  ┌────────────┐   ┌────────────┐                 ┌──────────────┐  │
│  │ StateStore │   │ rules.yaml │                 │ Telegram API │  │
│  │ (cursor)   │   │  (配置)    │                 │ (外网 HTTPS) │  │
│  └────────────┘   └────────────┘                 └──────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

数据流是一条单向流水线,四个核心组件用 `asyncio.Queue` 串联,天然背压。

---

## 4. 组件设计

### 4.1 BridgeClient — 事件消费(可靠性核心)

职责: 与 Bridge 保持长连接,保证事件**不丢不重**地进入流水线。

- **WS 优先**: 连接 `ws://<bridge-host>:17891/v1/events`(Bearer token 认证),实时接收 `{ type: "event", cursor, event }` 信封。
- **有界背压**: websockets 接收缓冲使用 16-frame high-water mark,再进入有界应用队列;reader 结束时先排空已入队帧,不依赖可能在满队列上阻塞的 sentinel。
- **REST 补齐**: 每次(重)连接时,先用持久化的 `last_acked_cursor` 调 `GET /v1/events?after=...&limit=500` 做 replay,循环翻页直到 `has_more == false`,再切到 WS 实时流。利用 Bridge 已有的 `next_cursor` / `has_more` / `buffer_latest_cursor` 语义。
- **去重**: cursor 是 SHA-256 稳定值。维护一个近期 cursor 集合(LRU,容量 ≈ Bridge buffer 大小),replay 与 WS 交界处的重复事件在此丢弃。
- **重连策略**: 指数退避(1s → 2s → … → 60s 封顶)+ jitter。WS 心跳超时(Bridge 有 heartbeat)视为断线。
- **cursor 推进原则(关键)**: `last_acked_cursor` 只在事件**成功发送到 Telegram 之后**、被规则明确 drop、或带可信 cursor 的确定性毒事件已持久化到死信之后推进。这样进程崩溃/重启后,从上次 ack 点 replay,实现 at-least-once。

已知风险: Bridge 的 replay buffer 有限(`DISCORD_BRIDGE_BUFFER_SIZE`)。若 forwarder 停机太久,buffer 淘汰会导致 replay 缺口。缓解:
1. forwarder 常驻(`restart: unless-stopped`)+ 掉线告警(见 §7);
2. 极端情况下 NDJSON 文件本身是全量源,可写一次性脚本手动补发(不做进主流程)。

### 4.2 Router — 规则引擎(转发的"可选性"就在这里)

职责: 对每个事件求值,输出 0..N 个 `(tg_chat_id, tg_thread_id?)` 目标。

- 规则**按序求值,首个匹配生效**(first-match wins),简单可预测。
- 匹配维度(全部可选,不写即通配):
  - `guild_id` / `channel_id`(支持列表)
  - `event_type`: CREATED / EDITED / DELETED / GHOST_PINGED
  - `author_id` / `author_name`(支持列表)
  - `keyword`: 正则,匹配消息文本
  - `is_dm`: 是否私信
- 动作:
  - `forward_to`: 一个或多个目标(chat_id + 可选 topic thread_id),一条消息可同时转发到多个 TG 目标
  - `drop`: 显式丢弃
- 可读控制字段: `channel_name` 保存频道显示名;`enabled` 默认 true。首个匹配但 disabled 的规则是终止式 drop,不会落入后续规则或 default。
- **default_action: drop**(推荐)。个人账户能看到的消息量很大,白名单式转发是安全默认;想全转的话把 default 改成 forward 即可。
- 配置热重载: 监听 `rules.yaml` mtime,变更后原子替换规则集,无需重启容器。热重载只立即改变消息路由;Telegram Topic 的 close/reopen 明确只在进程启动时同步。目标必须包含非空标量 `chat_id`,可选 `thread_id` 必须是非负整数;管理员输入错误统一按配置错误拒绝,保留旧的不可变快照且 watcher 继续运行。

### 4.3 Formatter — 消息格式化

职责: event → Telegram 消息文本(HTML parse mode)。

模板要素:
```
🆕 #channel-name @ ServerName          ← 事件类型图标 + 来源
👤 AuthorName
━━━━━━━━━━
消息正文 (HTML-escaped)
[附件: image.png]                       ← 媒体降级时的链接形式
```

- 事件图标: 🆕 CREATED / ✏️ EDITED / 🗑️ DELETED / 👻 GHOST_PINGED
- EDITED 尽量展示 before → after(collector 的 NDJSON 里若含旧内容);DELETED 展示被删内容 —— 这正是用 MessageLogger 的价值。
- 正文超长时按 Telegram 的 UTF-16 code-unit 口径执行 caption 1024 / text 4096 截断,并保持 HTML 标签闭合;媒体降级时先为完整 fallback 条目预留空间。只有具备 host、无空白/控制字符的合法 HTTP(S) URL 可进入 `href`,其他值固定降级为转义后的纯文本 `Attachment unavailable`,不会污染同一消息中的正常正文或合法链接。
- 所有用户内容先 HTML-escape,再将 Discord 的 `#`-`######` 标题与 `***粗斜体***` / `**粗体**` / `*斜体*` 安全转换为 Telegram HTML;未支持的 Markdown 保持原文。
- collector 未提供频道/服务器名称时使用对应 ID 展示;贴纸显示转义后的名称及 Discord CDN 链接。Discord embed 的 author/title/description/fields/footer/source link 按稳定顺序转义展示,重复正文不重复渲染。

### 4.4 MediaHandler — 媒体转发

- 按 `attachments → embed image → embed images[] → thumbnail` 的稳定顺序提取媒体(跳过 video),按 URL 去重并下载。仅允许 HTTPS:443 的精确主机 `cdn.discordapp.com`、`media.discordapp.net`、`images-ext-1.discordapp.net`、`images-ext-2.discordapp.net`、`pbs.twimg.com`;仍执行公共 DNS、无重定向、超时和大小检查。
- 按 MIME 分发: 图片 → `sendPhoto`,视频 → `sendVideo`,其他 → `sendDocument`;多附件用 `sendMediaGroup`。
- **任何失败(下载超时 / 403 / 超限)降级为文本消息 + URL 链接**,绝不因媒体失败丢掉整条转发。
- 注意: Discord CDN 链接带过期签名(`ex`/`is`/`hm` 参数),所以下载要在收到事件后**立即**做,不能积压后再下。

### 4.5 TgSender — 发送队列与限流

- 单 worker 从队列取任务发送(简单起见先不做并发,消息量大再优化)。
- 令牌桶限流: 全局 ~30 msg/s,**单 chat ~20 msg/min**(TG 对群组的硬限制,这是最容易踩的坑);突发容量与持续 refill rate 分离,允许单个最多 10 项的媒体组,不抬高配置的持续速率。
- 429 处理: 读取 `retry_after` 精确等待后重试。
- 网络错误: 重试 3 次(指数退避),仍失败则记 `failed-events.ndjson` 死信文件 + 日志告警,然后**继续推进 cursor**(不让一条毒消息卡死整个流水线)。
- 启动 Topic 同步只使用可逆的 `closeForumTopic` / `reopenForumTopic`,排除 General Topic,绝不删除历史。health endpoint 先启动;同步总预算 60 秒,单次 `retry_after` 最多 60 秒,预算耗尽后记录元数据告警并继续启动消息流水线。

### 4.6 StateStore

- 内容包含: `last_acked_cursor`、恢复计划、少量运行统计和当前规则所管理 Topic 的成功 `topic_states`。启动同步会原子裁剪规则中已不存在的 Topic key,但不修改 cursor、bootstrap 或 in-flight 恢复数据。Telegram 没有可用的 Topic 状态查询,因此该字段记录本程序上一次成功操作/首次升级基线,不是服务端绝对真相。
- 实现: 单个 JSON 文件,原子写(write temp + rename),挂载到 Docker volume。消息量不构成用 SQLite 的理由。
- 目标死信采用稳定 identity 和可恢复幂等终态转换: 若死信 append+fsync 后、状态终态持久化前崩溃,重启会识别已有记录并只补写目标终态,不重发、不重复追加死信;其他目标仍保持独立 pending。该协议不宣称两个文件之间具备原子事务。
- `state.json` 的 bootstrap/in-flight 部分以及 `failed-events.ndjson` 可包含完整 Discord 事件和 Telegram 目标 ID。bootstrap/in-flight payload 必须保留到对应顺序恢复步骤持久化完成,提前裁剪会破坏冻结决策和有序恢复,因此当前不做最小化。
- 死信按大小和数量有限轮转:下一条完整记录会使 active 超过 `dead_letter_max_bytes` 时,先将完整记录写入同目录 `.pending` 并 file-fsync,再将 active 原子 replace 为 `.1`、旧 `.1..N-1` 依次后移,最后把 `.pending` 原子提升为 active。启动后的首次 append 会先完成中断的 pending rotation。仅保留 `dead_letter_backup_count` 个 backup。默认 32 MiB × (active + 2 backup),稳态通常总量约 96 MiB;中断轮转期间可临时多一个 pending generation。单条超限记录不会被截断或静默脱敏,故 active 可超过阈值。所有 retained/pending 文件保持 0600,namespace 变更后 fsync 父目录,并拒绝 symlink/非普通文件。
- 恢复首次需要 identity 时以有界 chunk/line parser 一次扫描 active、全部 retained rotation 以及 pending。target record 把稳定 identity 序列化在完整 payload 之前,因此即使记录超过普通 line parser 上限,仍可在有界 prefix 内恢复。这里不做按时间删除:append+fsync 后、state persist 前的崩溃可能仍依赖较旧 rotation 中的 identity;按年龄过期会重新引入重复转发/重复死信风险。

### 4.7 日志与配置

- loguru + 现有 `LoggerManager.py` 风格(自定义 level 图标),与 monitor suite 一致。
- 配置分层: `config.yaml`(静态: bridge 地址、限流参数)+ `rules.yaml`(路由规则,可热重载)+ `.env`(密钥: TG bot token、bridge token)。
- **日志中不打印消息正文**(与 Bridge 项目验证时的隐私纪律一致),只打 guild/channel/事件类型/cursor 前 8 位。

---

## 5. Windows ↔ WSL 网络边界(部署前置条件)

Bridge 目前只绑 `127.0.0.1:17891`,WSL2 NAT 模式下容器访问不到。两个方案:

### 已验证方案: Bridge 保持 loopback + Docker Desktop host.docker.internal

Bridge 继续绑定 `127.0.0.1:17891`。Docker Desktop 的 `host.docker.internal` 已实测可直接访问,无需改绑 `0.0.0.0`,也无需额外 firewall 或 portproxy。

### 方案 B: netsh portproxy(Bridge 保持 loopback)

`netsh interface portproxy add v4tov4 listenaddress=<WSL网卡IP> listenport=17891 connectaddress=127.0.0.1 connectport=17891`

- 优点: Bridge 完全不动。
- 缺点(已有经验教训): portproxy 重启持久,但 **WSL NAT 子网会漂移**,漂移后规则失效需重配。

Bearer token 通过 `.env` 注入容器,不挂载 Windows token 文件。当前 Compose `env_file` 实测保留单个 `$`,不应双写。修改 `.env` 后必须执行 `docker compose up -d`,不能只 `docker restart`。

---

## 6. 配置文件示例

### rules.yaml

```yaml
# first-match wins, 从上到下求值
rules:
  - name: "trading-signals 全事件 → 交易信号频道"
    match:
      guild_id: "<DISCORD_GUILD_ID>"
      channel_id: ["<DISCORD_CHANNEL_ID_A>", "<DISCORD_CHANNEL_ID_B>"]
    forward_to:
      - chat_id: "<TELEGRAM_CHAT_ID>"

  - name: "重要人物的消息 → 私人提醒群 (含 topic)"
    match:
      author_id: ["<DISCORD_AUTHOR_ID>"]
      event_type: [CREATED, DELETED]      # 只关心新消息和撤回
    forward_to:
      - chat_id: "<TELEGRAM_CHAT_ID>"
        thread_id: 42                      # TG 超级群 topic

  - name: "所有 DM → 私人提醒群"
    match:
      is_dm: true
    forward_to:
      - chat_id: "<TELEGRAM_CHAT_ID>"

  - name: "噪音频道显式丢弃"
    match:
      channel_id: ["<DISCORD_CHANNEL_ID_C>"]
    action: drop

default_action: drop    # 未匹配的一律不转发(白名单模式)
```

### config.yaml

```yaml
bridge:
  url: "http://host.docker.internal:17891"  # Docker Desktop 已实测直达 Windows loopback
  reconnect_max_backoff_s: 60

telegram:
  rate_limit_global_per_s: 25
  rate_limit_per_chat_per_min: 18        # 留安全余量
  media_max_bytes: 20971520              # 20 MiB
  media_download_timeout_s: 15

state:
  path: /data/state.json
  dead_letter_path: /data/failed-events.ndjson
  dead_letter_max_bytes: 33554432       # 32 MiB active threshold
  dead_letter_backup_count: 2           # active + 2 backups, normally about 96 MiB total
```

### .env(不入库)

```
TG_BOT_TOKEN=...
BRIDGE_TOKEN=...        # 复制自 bridge-token.txt; 当前 env_file 实测保留单个 $,不要双写
```

---

## 7. 可靠性与可观测性小结

| 故障场景 | 行为 |
|---|---|
| forwarder 崩溃/重启 | 从 `last_acked_cursor` REST replay 补齐,cursor 去重防重复 |
| Bridge 重启 | WS 断线 → 指数退避重连 → replay 补齐 |
| Bridge buffer 淘汰(停机过久) | 持久化 gap 边界并 best-effort 告警,丢弃当前 WS epoch 后重连 |
| 可信 cursor 的 schema/payload 毒事件 | 先持久化死信,再按队列顺序推进 cursor;后续事件继续 |
| 无可信 cursor 的坏 JSON/帧 | 断开当前 session 并按退避策略重连,不猜测 cursor |
| TG 429 | 按 `retry_after` 等待重试,队列自然背压 |
| 单个目标发送连续失败 | 以稳定 identity 进死信并转终态;append 后崩溃会扫描 active + retained rotations 幂等恢复,其他目标不受影响 |
| 媒体下载失败 | 降级为文本 + 链接 |
| Discord/Windows 整体离线 | Bridge 无事件,forwarder 空转;恢复后 collector NDJSON 续写,自动跟上 |

可观测性(最小集):
- Docker healthcheck: 进程内简单 HTTP `/healthz`。连接且空闲保持 200;断连满 300 秒,或连接正常但存在队列/in-flight 工作且 durable cursor 300 秒无进展时返回 503。cursor 推进或 outstanding 清空恢复 200;响应包含非敏感 `stall_seconds` / `reason`。
- 心跳自监控: 每个 unhealthy episode 最多向管理群发一条告警,并区分 Bridge 断连与 forwarding pipeline stall。

---

## 8. 当前目录结构

```
discord-tg-forwarder/
├── ARCHITECTURE.md           # 本文档
├── README.md                 # 使用与运维说明
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── config.yaml
├── rules.yaml
├── src/
│   ├── main.py               # asyncio 入口, 组装流水线
│   ├── bridge_client.py      # WS + REST replay + cursor 管理
│   ├── router.py             # 规则引擎 + 热重载
│   ├── formatter.py          # 事件 → TG 消息模板
│   ├── media.py              # CDN 下载 + 降级
│   ├── tg_sender.py          # 队列 + 令牌桶 + 429/死信
│   ├── state.py              # 原子 JSON 状态
│   └── LoggerManager.py      # 复用 monitor suite 日志模式
├── tests/
└── data/                     # volume: state.json / failed-events.ndjson
```

依赖(刻意精简): `httpx`(REST + TG API + CDN 下载)、`websockets`(Bridge WS)、`PyYAML`、`loguru`。**不引入** python-telegram-bot 之类的重框架 —— 只用到 sendMessage/sendPhoto/sendDocument 几个端点,httpx 直调足矣。

---

## 9. 实施状态

| 阶段 | 状态 | 验收证据 |
|---|---|---|
| **M0 网络打通** | 已完成 | 容器内访问 Bridge `/v1/health` 得到 200 |
| **M1 MVP** | 已完成 | WS-first replay、持久 cursor 与纯文本流水线测试通过 |
| **M2 路由** | 已完成 | first-match、热重载与 default drop 测试通过 |
| **M3 媒体** | 已完成 | 即时准备、上传、SSRF 防护与链接降级测试通过 |
| **M4 加固** | 已完成 | 限流、429、死信、healthcheck 与掉线告警测试通过 |

---

## 10. 已关闭的设计问题

1. **EDITED before/after**: 真实 NDJSON 已确认包含 `editHistory`,格式化器展示 before → after。
2. **Windows 宿主入口**: 当前 Docker Desktop 已实测 `host.docker.internal` 可直达 loopback Bridge。
3. **回复引用**: 已实现常见字段别名的 HTML 转义原文摘要;贴纸保持文本/链接降级语义。
4. **Bridge buffer**: 保持上游默认 10,000;超出保留窗口时执行已确认的 409 告警并跳到 ready 边界策略。
