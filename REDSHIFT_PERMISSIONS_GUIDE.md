# Redshift 权限和调试指南

## 🚀 快速参考 - 100% 兼容的查询

**这些查询在所有 Redshift 版本上都能工作，不需要特殊权限：**

```sql
-- ✅ 1. 验证连接
SELECT current_database(), current_user, pg_backend_pid(), now();

-- ✅ 2. 查看表信息
SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public';

-- ✅ 3. 验证数据是否加载
SELECT COUNT(*) FROM public.your_table_name;

-- ✅ 4. 查看表结构
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'your_table';

-- ✅ 5. 检查 COPY 错误（你自己的）
SELECT query, filename, err_reason
FROM stl_load_errors
WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
  AND starttime > CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY starttime DESC LIMIT 10;
```

**⚠️ 避免使用这些可能不兼容的查询：**
- ❌ `SELECT locktype FROM pg_locks` - `locktype` 列可能不存在
- ❌ `SELECT * FROM stv_recents` - 需要超级用户权限
- ❌ `SELECT * FROM stv_inflight` - 需要超级用户权限

**✅ 推荐：使用应用层诊断工具**
```bash
python scripts/debug_redshift_copy.py --env us_dw
```

---

## 🔐 Redshift 系统表权限说明

### **系统表访问权限要求**

Redshift 的系统表（`STV_*`, `STL_*`, `SVL_*`, `SVV_*`）需要**不同级别的权限**：

#### **需要超级用户权限的系统表** ❌
这些表需要 `SUPERUSER` 权限或特定的系统权限才能访问：

```sql
-- 需要超级用户权限
STV_RECENTS          -- 当前运行的查询
STV_INFLIGHT         -- 正在执行的查询详情
STV_LOCKS           -- 锁信息
STV_SESSIONS        -- 会话信息
STV_WLM_SERVICE_CLASS_STATE  -- WLM队列状态
```

**错误示例**：
```
ERROR: permission denied for relation stv_recents
```

#### **普通用户可访问的系统视图** ✅
这些视图普通用户也可以访问：

```sql
-- 普通用户可访问
PG_TABLES           -- 表信息
PG_LOCKS            -- 自己会话的锁信息（部分）
PG_STAT_ACTIVITY    -- 自己的会话活动（部分）
INFORMATION_SCHEMA  -- 标准 SQL 信息模式视图
STL_LOAD_ERRORS     -- COPY错误（自己用户的）
```

---

## 🛠️ **无需系统表权限的调试方案**

### **方案 1: 使用应用层诊断工具** ⭐ **推荐**

我已经为你创建了诊断工具，运行方法：

```bash
# 基础诊断（测试连接、延迟、配置）
python scripts/debug_redshift_copy.py \
  --config config/connections.yml \
  --env us_dw

# 带 S3 COPY 测试（检测是否会卡住）
python scripts/debug_redshift_copy.py \
  --config config/connections.yml \
  --env us_dw \
  --test-s3 "s3://your-bucket/incremental/test.parquet"
```

**这个工具会检测**：
- ✅ Redshift 连接是否正常
- ✅ 网络延迟是否过高
- ✅ SSH 隧道是否稳定
- ✅ COPY 操作是否超时/卡住
- ✅ 表是否存在和数据是否加载成功

### **方案 2: 使用 pg_catalog 视图**

这些视图不需要特殊权限：

```sql
-- 1. 查看当前会话信息
SELECT
    current_database() as database,
    current_user as user,
    pg_backend_pid() as my_pid,
    now() as current_time,
    version() as version;

-- 2. 查看表信息
SELECT
    schemaname,
    tablename,
    tableowner,
    hasindexes,
    hasrules
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;

-- 3. 验证数据是否加载成功
SELECT COUNT(*) FROM public.your_table_name;

SELECT * FROM public.your_table_name LIMIT 5;

-- 4. 查看自己的锁（简化版，兼容 Redshift）
SELECT
    pid,
    relation,
    granted
FROM pg_locks
WHERE pid = pg_backend_pid();

-- 如果上面的查询也不支持，可以尝试：
-- SELECT * FROM pg_locks WHERE pid = pg_backend_pid();

-- 5. 查看 COPY 错误（只能看到自己用户的）
SELECT
    query,
    filename,
    line_number,
    colname,
    err_code,
    err_reason
FROM stl_load_errors
WHERE starttime > CURRENT_TIMESTAMP - INTERVAL '1 hour'
  AND userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
ORDER BY starttime DESC
LIMIT 20;
```

**⚠️ 注意：Redshift 兼容性问题**

Redshift 的 `pg_locks` 视图可能不完全兼容标准 PostgreSQL。如果上面的锁查询失败，有以下替代方案：

```sql
-- 方案 A: 查看 pg_locks 的实际结构
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'pg_locks'
ORDER BY ordinal_position;

-- 方案 B: 如果 pg_locks 不可用，跳过锁检查
-- 使用应用层诊断工具替代（推荐）

-- 方案 C: 简化查询，只查看基本信息
SELECT * FROM pg_locks LIMIT 5;
```

### **方案 3: 使用超时机制检测卡住**

在代码中添加超时检测（已在诊断工具中实现）：

```python
from src.utils.redshift_diagnostics import query_timeout

# 在 COPY 命令执行时添加超时
with query_timeout(300):  # 5分钟超时
    cursor.execute("COPY table FROM 's3://...' ...")

# 如果超时会抛出 TimeoutException
```

### **方案 4: 从应用日志分析**

检查应用日志中的关键信息：

```bash
# 查找 COPY 相关日志
grep -i "executing copy\|copy command" logs/*.log | tail -20

# 查找超时错误
grep -i "timeout\|timed out" logs/*.log

# 查找 SSH 隧道问题
grep -i "tunnel\|ssh" logs/*.log

# 查找 S3 访问错误
grep -i "s3.*error\|access denied\|403" logs/*.log
```

### **方案 5: CloudWatch 监控（AWS 控制台）**

如果你有 AWS 控制台访问权限，可以在 Redshift 控制台查看：

1. **Redshift Console** → **Queries and loads**
   - 查看运行中的查询
   - 查看 COPY 操作状态
   - 查看错误信息

2. **CloudWatch Metrics**
   - `DatabaseConnections` - 连接数
   - `PercentageDiskSpaceUsed` - 磁盘使用率
   - `CPUUtilization` - CPU使用率
   - `ReadLatency/WriteLatency` - I/O延迟

3. **Redshift Query Monitoring**
   - 查看慢查询
   - 查看队列等待时间
   - 查看WLM配置

---

## 📋 **如何请求最小必要权限**

如果需要向DBA申请权限，可以请求以下**最小权限集**：

### **选项 1: 只读监控视图访问** （最小权限）

```sql
-- 授予对特定系统视图的 SELECT 权限
GRANT SELECT ON stl_load_errors TO your_user;
GRANT SELECT ON svl_query_summary TO your_user;
GRANT SELECT ON svv_table_info TO your_user;
```

### **选项 2: 监控角色** （推荐）

```sql
-- 创建监控角色并授予权限
CREATE ROLE monitoring_role;

-- 授予系统视图访问
GRANT SELECT ON stl_load_errors TO monitoring_role;
GRANT SELECT ON stl_query TO monitoring_role;
GRANT SELECT ON svl_statementtext TO monitoring_role;
GRANT SELECT ON svv_table_info TO monitoring_role;

-- 将角色分配给用户
GRANT monitoring_role TO your_user;
```

### **选项 3: 创建自定义监控视图**

让 DBA 创建一个视图，普通用户可以访问：

```sql
-- DBA 创建监控视图
CREATE VIEW public.copy_monitoring AS
SELECT
    query,
    starttime,
    duration/1000000 as duration_seconds,
    querytxt
FROM stl_query
WHERE querytxt ILIKE '%COPY%'
  AND starttime > CURRENT_DATE - 1;

-- 授予访问权限
GRANT SELECT ON public.copy_monitoring TO your_user;
```

---

## 🔍 **实用调试命令（无需特殊权限）**

### **Redshift 兼容性说明** ⚠️

Redshift 基于 PostgreSQL 8.0.2，但做了大量修改。某些标准 PostgreSQL 视图可能：
- **列名不同**（如 `pg_locks` 的 `locktype` 列可能不存在）
- **完全不可用**（需要特殊权限）
- **返回数据格式不同**

**推荐做法**：
1. ✅ **优先使用应用层诊断工具**（不依赖系统表）
2. ✅ **测试查询前先检查视图结构**
3. ✅ **使用 `information_schema` 替代 `pg_*` 视图**

---

### **检查 COPY 是否成功**

```sql
-- 方法 1: 查看最近的 COPY 命令（从自己的查询历史）
-- 注意：stl_query 可能需要特殊权限，如果失败请使用方法 2
SELECT
    query,
    SUBSTRING(querytxt, 1, 100) as query_text,
    starttime,
    endtime,
    DATEDIFF(second, starttime, endtime) as duration_seconds
FROM stl_query
WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
  AND querytxt ILIKE '%COPY%'
ORDER BY starttime DESC
LIMIT 10;

-- 方法 2: 直接验证表数据（最可靠）✅ 推荐
SELECT
    'Data verification' as check_type,
    COUNT(*) as row_count,
    MIN(id) as min_id,
    MAX(id) as max_id
FROM public.your_table_name;

-- 方法 3: 检查最近插入的数据（如果有时间戳列）
SELECT
    COUNT(*) as recent_rows,
    MAX(created_at) as latest_timestamp
FROM public.your_table_name
WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour';
```

### **检查表数据**

```sql
-- 验证表存在
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE tablename = 'your_table'
  AND schemaname = 'public';

-- 检查行数
SELECT COUNT(*) as total_rows
FROM public.your_table;

-- 查看最近的数据（如果有时间戳列）
SELECT *
FROM public.your_table
ORDER BY created_at DESC
LIMIT 10;

-- 检查表大小
SELECT
    table_schema,
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name))) AS size
FROM information_schema.tables
WHERE table_name = 'your_table';
```

### **测试连接和性能**

```python
# 使用诊断工具
from src.utils.redshift_diagnostics import RedshiftDiagnostics
import psycopg2

conn = psycopg2.connect(...)
diagnostics = RedshiftDiagnostics(conn)

# 测试延迟
result = diagnostics.test_network_latency()
print(f"平均延迟: {result['avg_latency_ms']} ms")

# 测试 COPY（带超时）
result = diagnostics.test_s3_copy_simple(
    s3_uri="s3://bucket/file.parquet",
    aws_access_key="...",
    aws_secret_key="...",
    timeout_seconds=60
)

if result['timed_out']:
    print("❌ COPY 操作超时，可能卡住了")
else:
    print(f"✅ COPY 成功: {result['rows_loaded']} 行")
```

---

## 🔐 **解锁决策流程** ⭐ **重要**

### **步骤 1: 确定锁的所有者**

```sql
-- 查看你的 PID
SELECT pg_backend_pid();  -- 假设返回 1234

-- 查看锁的 PID
SELECT pid FROM pg_locks WHERE granted = true;  -- 假设看到 1073815845
```

### **步骤 2: 根据所有者选择方法**

```
┌─────────────────────────────────────┐
│ 锁的 PID 是你自己的？              │
│ (PID = pg_backend_pid())          │
└──────────┬──────────────────────────┘
           │
    ┌──────┴──────┐
    │             │
   是            否
    │             │
    ▼             ▼
┌───────────┐  ┌──────────────────┐
│ 情况 A    │  │ 情况 B           │
│ 自己的锁  │  │ 别人的锁         │
└─────┬─────┘  └────┬─────────────┘
      │             │
      ▼             ▼
 ✅ 使用:        ✅ 使用:
 ROLLBACK;      pg_cancel_backend()
 或 COMMIT;     或 pg_terminate_backend()
      │             │
      ▼             ▼
 不断开连接     会终止那个会话
 优雅释放锁     强制释放锁
```

### **情况 A: 自己的锁** ✅ **推荐方法**

```sql
-- 第一步：确认是自己的
SELECT pg_backend_pid();  -- 返回 1073815845

-- 第二步：检查锁
SELECT * FROM pg_locks WHERE pid = 1073815845;  -- 确实有锁

-- 第三步：释放锁（选一个）
ROLLBACK;  -- 取消所有未提交的更改，释放锁
-- 或
COMMIT;    -- 提交所有更改，释放锁

-- ✅ 优点：
--   • 不会断开连接
--   • 可以选择提交或回滚
--   • 更优雅、可控

-- ❌ 不要用：
-- SELECT pg_terminate_backend(1073815845);
-- （会断开你自己的连接）
```

### **情况 B: 别人的锁** ✅ **使用 terminate**

```sql
-- 第一步：确认不是自己的
SELECT pg_backend_pid();  -- 返回 1234（不同）

-- 第二步：查看锁是谁的
SELECT * FROM pg_locks WHERE pid = 1073815845;  -- 确实有锁

-- 第三步：尝试取消（较温和）
SELECT pg_cancel_backend(1073815845);

-- 等待 5-10 秒...

-- 第四步：如果还锁着，强制终止
SELECT pg_terminate_backend(1073815845);

-- ✅ 为什么用 terminate：
--   • 你不能在别人的会话里执行 ROLLBACK/COMMIT
--   • pg_terminate_backend() 会：
--     1. 终止那个会话
--     2. 自动回滚那个会话未提交的事务
--     3. 释放所有锁

-- ❌ 不能用：
-- ROLLBACK;  -- 只会影响你自己的会话，不是那个 PID
```

### **快速判断代码**

```sql
-- 一键判断应该用什么方法
SELECT
    CASE
        WHEN <锁的PID> = pg_backend_pid() THEN
            '这是你的锁，请使用 ROLLBACK 或 COMMIT'
        ELSE
            '这是别人的锁，请使用 pg_terminate_backend(' || <锁的PID> || ')'
    END as recommendation;

-- 实际例子：
SELECT
    CASE
        WHEN 1073815845 = pg_backend_pid() THEN
            '这是你的锁，请使用 ROLLBACK 或 COMMIT'
        ELSE
            '这是别人的锁，请使用 pg_terminate_backend(1073815845)'
    END as recommendation;
```

---

## 🚀 **快速故障排查流程**

### **步骤 1: 运行诊断脚本**

```bash
python scripts/debug_redshift_copy.py --env us_dw
```

### **步骤 2: 检查表数据**

```sql
-- 连接到 Redshift
psql -h localhost -p <tunnel_port> -U <user> -d <database>

-- 检查表
SELECT COUNT(*) FROM public.target_table;
```

### **步骤 3: 查看应用日志**

```bash
# 查看最近的 COPY 操作
tail -100 logs/sync.log | grep -i "copy"

# 查看错误
tail -100 logs/sync.log | grep -i "error\|failed"
```

### **步骤 4: 检查 watermark**

```bash
# 查看 S3 上的 watermark
aws s3 ls s3://your-bucket/watermark/

# 下载并查看
aws s3 cp s3://your-bucket/watermark/table_name_watermark.json -
```

---

## ❓ **常见问题和解决方案**

### **Q1: COPY 操作卡住不动怎么办？**

**A**: 使用诊断工具检测：
```bash
python scripts/debug_redshift_copy.py --env us_dw --test-s3 "s3://bucket/file.parquet"
```

如果超时，检查：
- SSH 隧道是否稳定
- S3 文件是否可访问
- Redshift 集群是否暂停

### **Q2: 如何知道 COPY 是否成功？**

**A**: 三种方法验证：

1. **查询表行数**
   ```sql
   SELECT COUNT(*) FROM public.target_table;
   ```

2. **查看应用日志**
   ```bash
   grep "✅ COPY command loaded" logs/sync.log
   ```

3. **检查 watermark**
   ```bash
   aws s3 cp s3://bucket/watermark/table_watermark.json -
   ```

### **Q3: 看不到 stv_recents 怎么查运行中的查询？**

**A**: 使用以下替代方案：

1. **在应用中添加超时检测**（已实现在诊断工具中）
2. **检查应用日志的时间戳**判断是否卡住
3. **使用 CloudWatch** 查看 Redshift 指标
4. **请求 DBA** 帮忙查看系统表

### **Q4: 没有超级用户权限，如何调试？**

**A**: 使用我创建的诊断工具：
- `scripts/debug_redshift_copy.py` - 自动检测问题
- `src/utils/redshift_diagnostics.py` - Python API

这些工具**不需要系统表权限**，通过应用层检测问题。

---

## 📝 **总结**

| 调试方法 | 需要权限 | 推荐度 | 适用场景 |
|---------|---------|--------|---------|
| 应用层诊断工具 | ❌ 不需要 | ⭐⭐⭐⭐⭐ | 首选，自动检测 |
| pg_catalog 视图 | ❌ 不需要 | ⭐⭐⭐⭐ | 查看表和数据 |
| 应用日志分析 | ❌ 不需要 | ⭐⭐⭐⭐ | 追踪执行流程 |
| CloudWatch 控制台 | AWS 控制台 | ⭐⭐⭐ | 可视化监控 |
| stl_load_errors | ❌ 不需要* | ⭐⭐⭐ | 查看 COPY 错误 |
| 系统表 (stv_*) | ✅ 需要超级用户 | ⭐⭐ | 最详细信息 |

*注：stl_load_errors 只能看到自己用户的错误

**推荐使用顺序**：
1. 运行 `debug_redshift_copy.py` 诊断脚本
2. 检查应用日志
3. 查询 `stl_load_errors` 表
4. 连接 Redshift 验证表数据
5. 如果仍无法定位，请求 DBA 协助
