# dynamic-indexserver 代际发布与清空栅栏 - 产品需求文档

## Overview

- **Summary**：为 push 模型的 `zoekt-dynamic-indexserver` 建立"隔离构建 + 代际发布 + 清空栅栏 + 崩溃收敛"机制。每个 `/index` 请求固定仓库编号与来源/目标提交，在私有的裸仓库与暂存索引目录中完成 clone、fetch 与 shard 校验，再按仓库代际一次性发布到共享索引目录；`/truncate` 先持久化栅栏、取消并等待在跑任务，再清空磁盘；进程重启后从磁盘候选与任务收据收敛。
- **Purpose**：消除"索引任务仍在 fetch/index 时并发清空仓库目录，旧进程随后把 shard 写回已清空目录"导致的接口成功结果与磁盘可查询集合不一致；同时保证同仓库并发只由较新完整结果生效、失败可幂等重试、崩溃与清空后旧任务不复活。
- **Target Users**：调用 dynamic-indexserver 推送索引与执行全量清空的运维系统/操作者，以及依赖 `/`、`/metrics` 判断健康与观测指标的平台。

## Goals

- 每个推送请求在登记时即固定 `RepoID + CloneURL + 来源提交 + 目标提交`，并在隔离目录中完成全部可变操作。
- 共享 `indexDir` 的唯一写入途径是"校验通过后的代际发布"；同仓库并发请求只允许序号较新的完整结果发布，较旧完成者得到可重试的 409。
- `/truncate` 先阻止新任务与新发布，取消（kill 子进程）并等待全部已登记任务退出，然后再删除；成功返回后任何旧任务都不能发布 shard。
- clone 失败、索引超时、shard 已改名但响应失败三类情形分别持久化可重试结果；重试能够识别已发布代际，不重复构建。
- 进程在任意阶段中断后，重启时依据磁盘候选（隔离的裸仓库、暂存 shard）与任务收据收敛到一致状态；清空成功（含清空过程中崩溃）后，旧收据/旧任务不能复活任何仓库。
- 健康检查、指标、不同仓库并行、既有请求格式与普通单次索引行为保持兼容。

## Non-Goals

- 不改造 pull 模型的 `cmd/zoekt-sourcegraph-indexserver`（含其 `DeleteAllData`、queue、cleanup、merge）。
- 不修改 `zoekt-git-clone`、`zoekt-git-index`、`gitindex`、`index` 等被调命令/库的行为与 shard 文件格式。
- 不引入跨实例分布式协调；状态仅持久化在本机保留目录，单进程假设不变。
- 不新增调用方必填字段；不实现基于认证/鉴权的多租户隔离（dynamic-indexserver 现状无租户概念）。
- 不处理 compound shard / shard 合并（dynamic-indexserver 不启用 merge）。

## Background & Context

现状代码位于 [main.go](file:///Users/ding/Documents/swe/0910152/project-02/cmd/zoekt-dynamic-indexserver/main.go)：

- `POST /index`（请求体 `{CloneURL, RepoID}`，`DisallowUnknownFields`）同步执行：
  1. `zoekt-git-clone -dest <repoDir> -name <RepoID> -repoid <RepoID> <CloneURL>` → 裸仓库固定落在共享 `<repoDir>/<RepoID>.git`；
  2. `git -C <repoDir>/<RepoID>.git fetch`；
  3. `zoekt-git-index -index <indexDir> <repoDir>/<RepoID>.git` → shard 直接写入共享 `<indexDir>`。
- `POST /truncate` 直接 `emptyDirectory(repoDir)` 与 `emptyDirectory(indexDir)`，与在跑任务之间无任何同步；net/http 每请求一个 goroutine，竞态真实存在：
  - fetch 中仓库目录被清空，clone/fetch 随后失败或在被删目录上继续；
  - 仓库目录删除后，已在运行的 `zoekt-git-index` 仍把 shard rename 进 `indexDir`，随后又被清空（或清空先返回、shard 后出现），接口结果与磁盘集合不一致；
  - 发布完成但 HTTP 响应失败时，客户端重试会全量重建。
- shard 由 `index.Builder` 先写 `*.tmp` 再 rename 为 `<前缀>_v<格式版本>.<分片号>.zoekt`（见 [shard_builder.go#L731](file:///Users/ding/Documents/swe/0910152/project-02/index/shard_builder.go#L731) 与 [builder.go#L779](file:///Users/ding/Documents/swe/0910152/project-02/index/builder.go#L779)）；dynamic-indexserver 以 RepoID 的十进制字符串作为仓库名（`-name 100`），故 shard 前缀即 `<RepoID>`，可按 `<RepoID>_v*.zoekt` 精确定位一个仓库的全部 shard。
- 可复用的现成能力：`index.ReadMetadataPath`（校验 shard 内 repo/分支元数据）、`index.IndexFilePaths`（shard 及 `.meta` sidecar 全部路径）、POSIX rename 同目录原子替换。
- 现有测试通过包级变量 `executeCmd` 注入命令执行；`TestIndexRepository` 断言三条命令的精确参数，重构后需按新隔离路径更新。

### 已确认的决策（用户澄清）

1. 范围仅限 `cmd/zoekt-dynamic-indexserver`。
2. 来源/目标提交由服务端自动解析并固定：来源提交 = fetch 前 `git rev-parse HEAD`（首次克隆也有 clone 产生的 HEAD；无法解析时记为空串），目标提交 = fetch 后 `git rev-parse HEAD`；请求体保持 `{CloneURL, RepoID}` 不变。
3. 同仓库并发、较旧任务在较新代际发布后才完成：不发布旧结果，返回 409 可重试错误；客户端重试时命中已发布代际，直接成功。
4. truncate 策略：持久化栅栏 → cancel 在跑任务（kill git/zoekt-git-index 子进程）→ 有界等待全部退出 → 删除目录内容；栅栏先于删除落盘，崩溃重启后继续完成清空。

## Functional Requirements

- **FR-1 任务登记与三元组固定**：`/index` 接受请求后在管理器中登记任务，分配单调递增序号与任务 ID，写入任务收据（epoch、seq、RepoID、CloneURL、阶段、时间戳）；clone 前/fetch 后分别解析并固定来源、目标提交到收据。
- **FR-2 隔离执行环境**：每个任务拥有独立工作目录（隔离裸仓库父目录 + 隔离暂存索引目录）；`zoekt-git-clone`、`git fetch`、`zoekt-git-index` 的所有写入只发生在该任务目录内，发布前不修改共享 `repoDir`/`indexDir`。
- **FR-3 shard 校验**：索引完成后、发布前，必须对暂存目录内每个 shard 读取元数据校验：至少 1 个 shard；每个 shard 含且仅含本 RepoID（仓库名为 RepoID 字符串）；HEAD 分支版本等于已固定的目标提交。任一不满足则禁止发布。
- **FR-4 代际发布**：发布是 `indexDir` 的唯一写入途径；在管理器临界区内复核 epoch 与序号仲裁后，把暂存 shard（含 `.meta` sidecar）rename 到 `indexDir`（跨设备时回退 copy+fsync+remove），覆盖同名旧 shard、删除上一代多余 shard，并原子写入每仓库代际记录。发布与写响应之间持久化"已发布"收据。
- **FR-5 同仓库并发仲裁**：同一 RepoID 的多个任务可并发执行；发布时仅当任务序号大于该仓库已发布代际序号才允许发布，否则本次构建被丢弃且返回 409（响应体保持 `{Success:false, Error}` 并附可重试标记）。
- **FR-6 不同仓库并行**：不同 RepoID 的任务通过有界信号量并行执行（新增 `-index_concurrency`，默认 `runtime.NumCPU`，0 表示不限），彼此目录、发布互不阻塞。
- **FR-7 清空栅栏**：`/truncate` 先原子持久化新 epoch（purgePending=true），此后新任务登记被拒（503 可重试）、发布一律失败；随后 cancel 全部在跑任务的 context（终止其子进程）并等待活动任务数归零（有界宽限），再删除 `indexDir` 全部条目、`repoDir` 除保留状态目录外的全部条目、全部任务工作目录/收据与代际记录，最后清除 purgePending。仅在删除完成后返回 `{Success:true}`。
- **FR-8 三类可重试结果**：
  - clone 失败：收据记录 `clone_failed`，不保留可用候选，HTTP 500 + 可重试标记；
  - 索引超时（或被清空 cancel）：收据记录 `index_timeout`/`cancelled`，HTTP 500/503 + 可重试标记；
  - shard 已改名发布但响应失败：收据与代际记录均为已发布；客户端重试时 FR-9 生效。
- **FR-9 重试幂等**：新请求 fetch 固定目标提交后、构建前，若该 RepoID 已发布代际的 CloneURL 与目标提交均相同，且代际记录中的 shard 全部存在并通过元数据校验，则跳过 `zoekt-git-index` 直接返回成功（响应可附"复用代际"标识，字段为追加而非修改既有字段）。
- **FR-10 启动恢复**：进程启动（开始接受索引任务前）加载 epoch/代际/收据：
  - purgePending=true：完成清空（删两个目录内容、tasks/、generations/）并清除标记；
  - epoch 不匹配的收据及其候选：删除；
  - 无收据的磁盘候选：删除；
  - 收据处于已校验/发布中且候选有效、序号新于已发布代际：前滚完成发布；
  - 已发布但缺代际记录：按收据与候选前滚补记；候选无效：清理半成品并保留/作废对应代际；
  - 代际记录指向的 shard 缺失：删除该代际记录（仓库视为未索引）；
  - nextSeq 取现存序号最大值之后。
- **FR-11 旧任务防复活**：epoch 递增后，任何携带旧 epoch 的发布动作（含重启后由旧收据触发的前滚）一律拒绝；清空成功后磁盘上不存在任何可被旧任务复用的收据或候选。
- **FR-12 兼容契约**：`GET /` 始终返回 200；`/metrics` 继续暴露 `zoekt_dynamic_indexserver_requests_total`（标签 method/route/code）；`POST /index` 仍接受且仅要求 `{CloneURL, RepoID}`；成功响应仍含 `Success:true`、`/truncate` 成功形状不变；外部命令序列仍为 `zoekt-git-clone` → `git fetch` → `zoekt-git-index`（仅路径变为隔离目录）；普通单次索引端到端可用。

## Non-Functional Requirements

- **NFR-1 正确性**：共享目录的所有变更经管理器单一临界区仲裁；不得存在"清空返回成功后仍有 shard 落盘"的交错；发布在 POSIX 同文件系统上为原子 rename 序列。
- **NFR-2 可恢复性**：任意阶段 kill -9 后重启，系统收敛到至多一个每仓库代际、无孤儿候选、无错误复活；恢复在开始接受 `/index` 前完成，`/` 与 `/metrics` 不被阻塞。
- **NFR-3 可观测性**：保留既有请求计数指标；新增任务阶段、活动任务数、发布仲裁（成功/过期拒绝/栅栏拒绝）、清空代际与恢复结果指标；关键仲裁与恢复事件有日志。
- **NFR-4 可测试性**：命令执行、提交解析、时钟等外部依赖保持可注入接口，使并发交错、超时、崩溃各阶段可在单测中确定性复现；不依赖真实远程 Git 服务。
- **NFR-5 磁盘边界**：失败任务的隔离工作目录被有界回收（收据可保留用于诊断，大体积候选删除）；状态目录位置明确、不被 truncate 误删。
- **NFR-6 性能**：除一次每仓库发布临界区与既有 clone/fetch/index 开销外，不引入额外全量索引；同仓库去重命中时不启动 `zoekt-git-index`。

## Constraints

- **Technical**：Go（module `github.com/sourcegraph/zoekt`）；仅使用标准库 + 模块内既有依赖（`index`、`prometheus/client_golang`）；状态文件以"临时文件 + fsync + rename + 目录 fsync"原子落盘；命令经 `exec.CommandContext` 执行以便 cancel 杀进程；遵循 `go vet`、gofmt 与 AGENT.md 构建命令（`go build ./cmd/...`、`go test ./... -short`）。
- **Business**：不得要求调用方修改请求体；健康检查与既有指标名称/标签不得消失。
- **Dependencies**：运行环境提供 `git`、`zoekt-git-clone`、`zoekt-git-index`（与现状相同）；`indexDir` 由本进程独占（现状 truncate 已如此假设）。

## Assumptions

- 状态目录默认 `<repoDir>/.zoekt-dynamic-state`（保留名称，truncate 不删除它），可由新增 `-state_dir` 覆盖；目录结构：
  - `epoch.json`：`{epoch, nextSeq, purgePending}`；
  - `generations/<RepoID>.json`：`{repoID, cloneURL, generation(=seq), sourceCommit, targetCommit, shards:[basename], publishedAt}`；
  - `tasks/<taskID>/receipt.json`；`tasks/<taskID>/repo/<RepoID>.git`（隔离裸仓库）；`tasks/<taskID>/index/`（暂存 shard）。
- 暂存 shard 的 basename 与最终名一致（`<RepoID>_v*.zoekt`），故发布即同文件名 rename 替换。
- 清空宽限超时后仍未退出的任务：由于发布必须进入管理器临界区并复核 epoch，宽限到达后删除目录不会被旧任务发布覆盖（旧任务发布必失败）；宽限仅需保证文件删除时没有进程仍在写任务私有目录之外的路径。
- 一个 RepoID 的 shard 不含多仓库（无 compound）；元数据校验以 shard 内唯一 repo 为准。
- HTTP 响应成功发送与否无法由服务端确认，因此"已发布"以收据/代际记录落盘为准，而非响应写入成功。

## Acceptance Criteria

### AC-1: 请求三元组固定与隔离执行
- **Type**: `rule`
- **Given**: 一个携带 `{CloneURL, RepoID}` 的 `/index` 请求
- **When**: 请求被接受并执行
- **Then**: 存在任务收据记录 epoch/seq/RepoID/CloneURL；来源提交在 fetch 前、目标提交在 fetch 后被解析并写入收据；clone、fetch、index 三条命令的目标路径全部位于该任务私有目录，共享 repoDir/indexDir 在发布前无新增/修改
- **Pass Condition**: 单测注入命令执行器断言命令路径全部落在 `tasks/<id>/` 下，且收据阶段与提交字段随执行推进更新
- **Evidence**: `tasks/<id>/receipt.json` 内容断言；命令参数历史断言

### AC-2: shard 校验通过后按代际一次发布
- **Type**: `rule`
- **Given**: 任务在暂存目录产出 shard
- **When**: 索引命令返回后
- **Then**: 仅当每个暂存 shard 的元数据满足 RepoID 一致、仓库名=RepoID 字符串、HEAD 版本=目标提交且 shard 数≥1 时才发布；发布在单一临界区内完成 rename（含 sidecar）、旧代际多余 shard 删除、代际记录原子写入；共享 indexDir 不存在除发布外的写入路径
- **Pass Condition**: 构造合法与非法（错误 RepoID / 错误提交 / 空目录）三类暂存结果，分别断言发布成功或拒绝；发布后 indexDir 恰含当代 shard，generations 记录与磁盘一致
- **Evidence**: 单测中文件系统状态与代际记录断言

### AC-3: 同仓库并发仅较新完整结果生效
- **Type**: `rule`
- **Given**: 同一 RepoID 两个并发任务 A（序号小）、B（序号大），B 先完成发布、A 后完成
- **When**: A 尝试发布
- **Then**: A 的 shard 不进入 indexDir，A 的 HTTP 响应为 409 且响应体含 `Success:false` 与可重试标记；indexDir 中只有 B 代际的 shard；A 重试时按 AC-7 复用成功
- **Pass Condition**: 用可阻塞命令执行器确定性制造"B 先发布、A 后到"交错，断言状态码、indexDir 文件集合与代际序号
- **Evidence**: 并发单测断言

### AC-4: 不同仓库并行且互不阻塞
- **Type**: `rule`
- **Given**: 多个不同 RepoID 的 `/index` 请求同时到达
- **When**: 并发度上限允许时
- **Then**: 任务并行推进（可注入执行器观测到重叠执行），全部成功发布到各自文件名；信号量满时后续任务排队，0 表示不限
- **Pass Condition**: 单测用闸门证明 N 个不同仓库任务在时间上重叠且最终全部有代际记录；超过并发度时同时执行数不超过上限
- **Evidence**: 并发单测计时/闸门断言

### AC-5: truncate 栅栏、取消、等待后清空
- **Type**: `rule`
- **Given**: 一个长时间运行（阻塞在 fetch/index）的索引任务期间发起 `/truncate`
- **When**: truncate 执行
- **Then**: 栅栏（epoch+1, purgePending）先于任何删除落盘；栅栏后新 `/index` 返回 503；在跑任务 context 被取消且子进程收到终止；truncate 等待活动任务归零后才删除；成功响应后 indexDir 无 shard、repoDir 无裸仓库、tasks/ 与 generations/ 为空、purgePending=false；此后旧任务尝试发布被拒
- **Pass Condition**: 阻塞式单测复现"任务进行中 truncate"，断言取消发生、删除顺序、响应后目录为空且旧任务发布返回栅栏错误
- **Evidence**: 目录清单、epoch.json、任务收据状态断言

### AC-6: 三类失败分别留存可重试结果
- **Type**: `rule`
- **Given**: 分别注入：clone 命令失败；index 命令超过 indexTimeout；发布落盘成功后响应写出失败
- **When**: 三种场景发生
- **Then**: 持久化收据分别记录 `clone_failed`、`index_timeout`、`published`（响应失败不改变已发布事实），前两者响应为可重试错误，第三者磁盘代际已生效；再次发起相同请求时前两者重新执行构建、第三者不执行 `zoekt-git-index`
- **Pass Condition**: 三组单测断言收据类别、响应可重试标记与重试行为（命令是否再次出现）
- **Evidence**: 收据 JSON + 命令历史 + 状态码断言

### AC-7: 重试识别已发布代际且不重建
- **Type**: `rule`
- **Given**: RepoID 已发布代际目标提交为 C
- **When**: 相同 CloneURL 的新请求 fetch 后解析到目标提交仍为 C，且代际 shard 齐全且元数据一致
- **Then**: 不执行 `zoekt-git-index`，直接返回成功；若 shard 缺失或元数据不符则正常重建并发布新一代
- **Pass Condition**: 单测断言去重命中时命令历史无 `zoekt-git-index`；删除一个 shard 后重试触发重建
- **Evidence**: 命令历史与响应断言

### AC-8: 进程中断后从磁盘候选与收据收敛
- **Type**: `rule`
- **Given**: 分别在以下时刻构造"进程死亡"：clone 中途、index 完成已校验但未发布、发布 rename 进行到一半（部分 shard 已改名、无代际记录）、已发布且收据落盘但响应未确认；另有有候选无收据、有收据无候选两种孤儿
- **When**: 进程重启执行恢复
- **Then**: 有效且序号最新的已校验候选被前滚发布一次；半成品/孤儿候选被删除；缺 shard 的代际记录被移除；已发布事实不被重复发布；恢复后 nextSeq 单调；之后新请求行为正确
- **Pass Condition**: 五种场景各一个确定性单测（直接在文件系统布置状态后新建管理器），断言恢复后代际记录、indexDir 文件集合、tasks/ 残留
- **Evidence**: 恢复前后文件系统快照断言

### AC-9: 清空成功后旧任务不得复活仓库
- **Type**: `rule`
- **Given**: truncate 已成功（含 truncate 落栅栏后崩溃、重启自动完成清空两种路径）
- **When**: 旧 epoch 任务（重启后由旧收据恢复或内存中残留任务）尝试发布
- **Then**: 发布在临界区被拒绝；磁盘上无旧收据/候选被前滚；indexDir 保持为空；此后真正的新请求（新 epoch）可以正常索引
- **Pass Condition**: 单测布置旧 epoch 收据+候选后重启，断言无发布且候选被清除；再发新请求成功
- **Evidence**: 恢复结果与目录断言

### AC-10: 健康检查、指标、请求格式与普通索引兼容
- **Type**: `rule`
- **Given**: 重构后的服务
- **When**: 访问 `/`、`/metrics`；以原始 JSON 调 `/index`、`/truncate`；跑一次普通单仓库索引
- **Then**: `/` 返回 200；`/metrics` 含 `zoekt_dynamic_indexserver_requests_total{method,route,code}`；未知字段仍 400；成功体含 `Success:true`、清空成功形状不变；外部命令仍为 clone→fetch→index 且顺序不变；普通索引端到端成功并可在 indexDir 检出 shard
- **Pass Condition**: httptest 单测覆盖四个端点与指标文本；端到端单测用真实 `index.Builder` 产物或等价桩完成一次完整索引
- **Evidence**: HTTP 状态码/响应体/指标文本/命令序列断言

### AC-11: 全量构建与测试通过
- **Type**: `rule`
- **Given**: 全部改动完成
- **When**: 执行构建与测试
- **Then**: `go build ./...`、`go vet ./cmd/zoekt-dynamic-indexserver/...`、`gofmt -l`（该包无输出）、`go test ./cmd/zoekt-dynamic-indexserver/...` 全部通过；`go test ./... -short` 不引入回归
- **Pass Condition**: 上述命令退出码为 0
- **Evidence**: 命令输出记录于任务完成证据

### AC-12: 并发正确性与资源清理设计质量
- **Type**: `rubric`
- **Dimension**: 锁/栅栏设计是否单一、清晰、无死锁；任务工作目录生命周期是否有界且无泄漏；取消是否能传导到子进程
- **Scale**: 1-5
- **Anchors**: 1 = 存在多个未协调的共享写入点或可证明的死锁/泄漏；3 = 临界区边界基本正确但职责混杂或有边界泄漏；5 = 单一管理器仲裁、锁顺序明确、context 取消直达子进程、任务目录必被回收
- **Pass Threshold**: >= 4
- **Evidence**: 代码审查 + AC-3/AC-5/AC-6 交错测试

### AC-13: 崩溃一致性与磁盘布局清晰度
- **Type**: `rubric`
- **Dimension**: 状态文件原子性、发布/清空/恢复三条路径的不变量是否自洽且可推理
- **Scale**: 1-5
- **Anchors**: 1 = 崩溃后可产生复活或双代际；3 = 常见崩溃点收敛但边界场景靠侥幸；5 = 每条交错都有明确的前滚/丢弃规则且由 AC-8/AC-9 测试锁定
- **Pass Threshold**: >= 4
- **Evidence**: AC-8/AC-9 测试覆盖矩阵与恢复代码审查

### AC-14: 可观测性与兼容交付质量
- **Type**: `rubric`
- **Dimension**: 指标/日志对阶段、仲裁、清空、恢复的覆盖；对既有接口的兼容程度；代码组织是否便于后续维护
- **Scale**: 1-5
- **Anchors**: 1 = 既有指标消失或接口形状被破坏；3 = 兼容保留但新流程几乎不可观测；5 = 既有契约零破坏且新流程关键事件均可观测、文件划分清晰
- **Pass Threshold**: >= 4
- **Evidence**: AC-10 测试 + 指标/日志清单审查

## Open Questions

- 无（四项关键歧义已通过澄清确认，见 Background & Context）。
