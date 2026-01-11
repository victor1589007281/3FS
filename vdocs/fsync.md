# 3FS fsync 系统调用实现分析

## 1. 总体架构

```mermaid
graph TB
    subgraph "**应用层**"
        A[**用户进程<br/>fsync()/fdatasync()**]
    end
    
    subgraph "**FUSE层**"
        B[**hf3fs_fsync<br/>FUSE回调**]
        C[**flushAndSync<br/>刷新同步**]
        D[**sync()<br/>元数据同步**]
    end
    
    subgraph "**客户端层**"
        E[**MetaClient<br/>元数据客户端**]
    end
    
    subgraph "**元数据层**"
        F[**MetaOperator<br/>元数据服务**]
        G[**BatchedOp<br/>批量操作**]
        H[**FileHelper<br/>文件助手**]
    end
    
    subgraph "**存储层**"
        I[**StorageClient<br/>长度查询**]
    end
    
    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    
    style A fill:#ffe1e1,stroke:#333,stroke-width:2px,color:#000
    style B fill:#e1ffe1,stroke:#333,stroke-width:2px,color:#000
    style C fill:#e1f5ff,stroke:#333,stroke-width:2px,color:#000
    style D fill:#fff3e1,stroke:#333,stroke-width:2px,color:#000
    style E fill:#f5e1ff,stroke:#333,stroke-width:2px,color:#000
    style F fill:#fffacd,stroke:#333,stroke-width:2px,color:#000
    style G fill:#ffe1f5,stroke:#333,stroke-width:2px,color:#000
    style H fill:#e8e1ff,stroke:#333,stroke-width:2px,color:#000
    style I fill:#ffd7d7,stroke:#333,stroke-width:2px,color:#000
```

## 2. fsync vs fdatasync

```mermaid
graph TB
    subgraph "**fsync**"
        A1[**刷新写缓冲**]
        A2[**同步文件长度**]
        A3[**更新mtime/atime**]
        A4[**持久化元数据**]
    end
    
    subgraph "**fdatasync**"
        B1[**刷新写缓冲**]
        B2[**可选更新长度**]
        B3[**仅更新必要元数据**]
    end
    
    A1 --> A2
    A2 --> A3
    A3 --> A4
    
    B1 --> B2
    B2 --> B3
    
    style A1 fill:#ffe1e1,stroke:#333,stroke-width:2px,color:#000
    style A2 fill:#e1ffe1,stroke:#333,stroke-width:2px,color:#000
    style A3 fill:#e1f5ff,stroke:#333,stroke-width:2px,color:#000
    style A4 fill:#fff3e1,stroke:#333,stroke-width:2px,color:#000
    style B1 fill:#f5e1ff,stroke:#333,stroke-width:2px,color:#000
    style B2 fill:#fffacd,stroke:#333,stroke-width:2px,color:#000
    style B3 fill:#ffe1f5,stroke:#333,stroke-width:2px,color:#000
```

| **操作**       | **fsync**              | **fdatasync**          |
|----------------|------------------------|------------------------|
| 刷新写缓冲     | ✓                      | ✓                      |
| 更新文件长度   | 必须更新               | 配置控制               |
| 更新mtime      | ✓                      | 可选                   |
| 更新atime      | ✓                      | 可选                   |

## 3. 核心流程时序图

```mermaid
sequenceDiagram
    participant App as "用户进程"
    participant FUSE as "FUSE层"
    participant Meta as "MetaClient"
    participant MetaSvc as "元数据服务"
    participant Storage as "存储服务"
    
    App->>FUSE: **1. fsync(fd)**
    
    FUSE->>FUSE: **2. 检查是否为fdatasync**
    
    FUSE->>FUSE: **3. 获取inode信息<br/>inodeOf(*fi, ino)**
    
    FUSE->>FUSE: **4. 刷新写缓冲区<br/>flushBuf()**
    
    FUSE->>FUSE: **5. 检查是否需要sync<br/>syncver < writever?**
    
    alt **需要同步**
        FUSE->>FUSE: **6a. 收集动态属性<br/>atime, mtime, hintLength**
        
        FUSE->>Meta: **7. metaClient->sync()<br/>发送SyncReq**
        
        Meta->>MetaSvc: **8. RPC调用sync**
        
        MetaSvc->>MetaSvc: **9. distributor->getServer()<br/>确定负责节点**
        
        alt **本地处理**
            MetaSvc->>MetaSvc: **10a. runInBatch<br/>批量处理sync请求**
        else **转发到其他节点**
            MetaSvc->>MetaSvc: **10b. forward->forward<br/>转发请求**
        end
        
        MetaSvc->>MetaSvc: **11. BatchedOp::syncAndClose()<br/>合并sync/close请求**
        
        alt **需要更新长度**
            MetaSvc->>MetaSvc: **12a. 检查hintLength**
            
            alt **hintLength有效**
                MetaSvc->>MetaSvc: **13a. 直接使用hint**
            else **需要查询存储**
                MetaSvc->>Storage: **13b. queryLength()<br/>查询chunk长度**
                Storage-->>MetaSvc: **14. 返回文件长度**
            end
            
            MetaSvc->>MetaSvc: **15. 更新inode长度**
        end
        
        MetaSvc->>MetaSvc: **16. 更新mtime/ctime**
        
        MetaSvc->>MetaSvc: **17. inode->store(txn)<br/>持久化到FDB**
        
        MetaSvc-->>Meta: **18. 返回更新后的Inode**
        
        Meta-->>FUSE: **19. 返回结果**
        
        FUSE->>FUSE: **20. 更新本地缓存<br/>inode.update()**
        
        FUSE->>FUSE: **21. notify_inval_inode()<br/>通知内核缓存失效**
    end
    
    FUSE-->>App: **22. 返回0**
    
    rect rgb(255, 250, 205)
    Note over FUSE,Storage: **关键：fsync确保数据和元数据都已持久化**
    end
```

## 4. 函数调用链

```text
hf3fs_fsync() - src/fuse/FuseOps.cc:1691
├── 日志记录
│   └── XLOGF(OP_LOG_LEVEL, "hf3fs_sync(ino={}, datasync={}, pid={})", ...)
├── 操作计数
│   └── record(datasync ? "fdatasync" : "fsync", fuse_req_ctx(req)->uid)
├── 刷新并同步
│   └── flushAndSync(req, fino, datasync && !config->fdatasync_update_length(), SyncType::Fsync, fi)
│       └── ┌────────────┬────────────────────────┬───────────────────┐
│           │  **参数**   │  **含义**               │  **说明**          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  req       │  FUSE请求              │  包含用户信息      │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  fino      │  fuse inode号          │  文件标识          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  flushOnly │  是否仅刷新            │  fdatasync可能true │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  syncType  │  同步类型              │  Fsync/ForceFsync  │
│           └────────────┴────────────────────────┴───────────────────┘
└── 返回结果
    └── fuse_reply_err(req, 0)

flushAndSync() - src/fuse/FuseOps.cc:596
├── 获取inode
│   └── inodeOf(*fi, ino) - src/fuse/FuseOps.cc:606
├── 检查是否为文件
│   └── pi->inode.isFile()
├── 刷新写缓冲区
│   └── flushBuf(req, pi, wb->off, *wb->memh, wb->len, true) - src/fuse/FuseOps.cc:617
│       └── ┌────────────┬────────────────────────┬───────────────────┐
│           │  **步骤**   │  **操作**               │  **说明**          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  1         │  获取wbMtx锁           │  保证线程安全      │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  2         │  检查wb->len           │  有数据才刷新      │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  3         │  调用flushBuf          │  发送到存储节点    │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  4         │  清空wb->len           │  标记缓冲区已刷新  │
│           └────────────┴────────────────────────┴───────────────────┘
├── 检查是否仅刷新
│   └── if (flushOnly) return true
├── 移除dirty标记
│   └── d.dirtyInodes.lock()->erase(ino)
└── 同步元数据
    └── sync(userInfo, *pi, syncType) - src/fuse/FuseOps.cc:632

sync() - src/fuse/FuseOps.cc:517
├── 获取动态属性
│   └── inode.dynamicAttr.rlock()
│       └── ┌────────────┬────────────────────────┬───────────────────┐
│           │  **字段**   │  **含义**               │  **说明**          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  written   │  写入版本号            │  每次写入递增      │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  synced    │  周期同步版本          │  后台同步用        │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  fsynced   │  fsync版本             │  显式fsync用       │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  hintLength│  本地长度提示          │  避免查询存储      │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  atime     │  访问时间              │  本地缓存          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  mtime     │  修改时间              │  本地缓存          │
│           └────────────┴────────────────────────┴───────────────────┘
├── 检查是否需要同步
│   └── if (!force && syncver >= writever) return nullopt
├── 准备hintLength
│   └── fsync时可能不使用hint，强制查询实际长度
└── 调用MetaClient::sync
    └── d.metaClient->sync(userInfo, inode.inode.id, true, atime, mtime, hint)

MetaClient::sync() - src/client/meta/MetaClient.cc:941
├── 构造SyncReq
│   └── SyncReq(userInfo, inode, updateLength, atime, mtime, false, hint)
│       └── ┌────────────┬────────────────────────┬───────────────────┐
│           │  **字段**   │  **含义**               │  **说明**          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  inode     │  目标inode ID          │  要同步的文件      │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  updateLength│ 是否更新长度         │  fsync=true        │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  atime     │  访问时间              │  可选              │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  mtime     │  修改时间              │  可选              │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  hint      │  长度提示              │  优化查询          │
│           └────────────┴────────────────────────┴───────────────────┘
└── 发送请求
    └── retry(&IMetaServiceStub::sync, req)

MetaOperator::sync() - src/meta/service/MetaOperator.cc:319
├── 确定负责节点
│   └── distributor_->getServer(req.inode)
├── 本地处理
│   └── runInBatch<SyncReq, SyncRsp>(inodeId, std::move(req))
└── 转发请求
    └── forward_->forward<SyncReq, SyncRsp>(node, std::move(req))

BatchedOp::run() - src/meta/store/ops/BatchOperation.cc:57
├── 检查分布
│   └── distributor().checkOnServer(txn, inodeId_)
├── 加载inode
│   └── Inode::snapshotLoad(txn, inodeId_)
├── 处理sync和close
│   └── syncAndClose(txn, *inode)
├── 处理setAttr
│   └── setAttr(txn, *inode)
└── 持久化
    └── inode->store(txn)

BatchedOp::syncAndClose() - src/meta/store/ops/BatchOperation.cc:107
├── 合并所有sync请求
│   └── for (auto &waiter : syncs_) sync(inode, waiter.get().req, ...)
├── 合并所有close请求
│   └── for (auto &waiter : closes_) close(inode, waiter.get().req, ...)
├── 查询文件长度
│   └── queryLength(inode, hintLength, truncate) - src/meta/store/ops/BatchOperation.cc:260
│       └── ┌────────────┬────────────────────────┬───────────────────┐
│           │  **优化**   │  **条件**               │  **说明**          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  跳过查询  │  currLen>=hintLen      │  当前长度足够大    │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  使用hint  │  hint.truncVer==curr   │  版本匹配时        │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  查询存储  │  需要精确长度          │  truncate后必须    │
│           └────────────┴────────────────────────┴───────────────────┘
├── 更新文件长度
│   └── inode.asFile().setVersionedLength(*newLength)
└── 更新时间戳
    └── SetAttr::update(inode.mtime, UtcClock::now(), ...)

FileHelper::queryLength() - src/meta/components/FileHelper.cc:88
├── 创建FileOperation
│   └── FileOperation fop(*storageClient_, *rawRoutingInfo, userInfo, inode, recorder)
├── 查询所有chunk
│   └── fop.queryChunks(hasHole != nullptr, config_.dynamic_stripe())
│       └── ┌────────────┬────────────────────────┬───────────────────┐
│           │  **返回**   │  **字段**               │  **说明**          │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  length    │  文件实际长度          │  最大chunk偏移+长度 │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  totalChunkLen│ chunk总长度        │  检测空洞用        │
│           ├────────────┼────────────────────────┼───────────────────┤
│           │  totalNumChunks│ chunk总数         │  验证完整性        │
│           └────────────┴────────────────────────┴───────────────────┘
└── 返回长度
    └── co_return queryResult->length
```

## 5. 关键数据结构

### 5.1 RcInode::DynamicAttr

| **字段**       | **类型**               | **说明**                |
|----------------|------------------------|------------------------|
| written        | uint64_t               | 写入操作版本号          |
| synced         | uint64_t               | 周期同步版本号          |
| fsynced        | uint64_t               | fsync同步版本号         |
| writer         | flat::Uid              | 最后写入者UID           |
| dynStripe      | uint32_t               | 动态stripe数            |
| truncateVer    | uint64_t               | 截断版本号              |
| hintLength     | optional<VersionedLength> | 本地长度提示         |
| atime          | optional<UtcTime>      | 本地访问时间            |
| mtime          | optional<UtcTime>      | 本地修改时间            |

### 5.2 SyncReq

| **字段**       | **类型**               | **说明**                |
|----------------|------------------------|------------------------|
| user           | UserInfo               | 用户信息                |
| inode          | InodeId                | 目标inode               |
| updateLength   | bool                   | 是否更新长度            |
| atime          | optional<UtcTime>      | 访问时间                |
| mtime          | optional<UtcTime>      | 修改时间                |
| truncated      | bool                   | 是否截断操作            |
| hint           | optional<VersionedLength> | 长度提示             |

### 5.3 VersionedLength

| **字段**       | **类型**               | **说明**                |
|----------------|------------------------|------------------------|
| length         | uint64_t               | 文件长度                |
| truncateVer    | uint64_t               | 截断版本号              |

## 6. 同步类型

```mermaid
graph LR
    subgraph "**SyncType枚举**"
        A[**Lookup**<br/>查找时同步]
        B[**GetAttr**<br/>获取属性时同步]
        C[**PeriodSync**<br/>后台周期同步]
        D[**Fsync**<br/>显式fsync]
        E[**ForceFsync**<br/>强制fsync]
    end
    
    style A fill:#ffe1e1,stroke:#333,stroke-width:2px,color:#000
    style B fill:#e1ffe1,stroke:#333,stroke-width:2px,color:#000
    style C fill:#e1f5ff,stroke:#333,stroke-width:2px,color:#000
    style D fill:#fff3e1,stroke:#333,stroke-width:2px,color:#000
    style E fill:#f5e1ff,stroke:#333,stroke-width:2px,color:#000
```

| **类型**       | **触发场景**           | **行为差异**            |
|----------------|------------------------|------------------------|
| Lookup         | 目录项查找             | 不通知缓存失效          |
| GetAttr        | stat系统调用           | 不通知缓存失效          |
| PeriodSync     | 后台定时器             | 更新synced版本          |
| Fsync          | fsync系统调用          | 更新fsynced版本         |
| ForceFsync     | ioctl强制同步          | 忽略版本检查            |

## 7. 长度查询优化

```mermaid
graph TB
    subgraph "**长度查询决策流程**"
        A[**开始queryLength**]
        B{**有nextLength<br/>缓存?**}
        C{**hintLength<br/>有效?**}
        D{**currLen >=<br/>hintLen?**}
        E{**版本匹配?**}
        F[**返回currLength**]
        G[**返回hintLength**]
        H[**查询存储节点**]
        I[**返回查询结果**]
    end
    
    A --> B
    B -- 是 --> F
    B -- 否 --> C
    C -- 否 --> H
    C -- 是 --> D
    D -- 是 --> F
    D -- 否 --> E
    E -- 是 --> G
    E -- 否 --> H
    H --> I
    
    style A fill:#ffe1e1,stroke:#333,stroke-width:2px,color:#000
    style B fill:#e1ffe1,stroke:#333,stroke-width:2px,color:#000
    style C fill:#e1f5ff,stroke:#333,stroke-width:2px,color:#000
    style D fill:#fff3e1,stroke:#333,stroke-width:2px,color:#000
    style E fill:#f5e1ff,stroke:#333,stroke-width:2px,color:#000
    style F fill:#fffacd,stroke:#333,stroke-width:2px,color:#000
    style G fill:#ffe1f5,stroke:#333,stroke-width:2px,color:#000
    style H fill:#e8e1ff,stroke:#333,stroke-width:2px,color:#000
    style I fill:#ffd7d7,stroke:#333,stroke-width:2px,color:#000
```

### 7.1 hintLength机制

- **来源**：客户端在`finishWrite`时更新本地hintLength
- **格式**：`{length, truncateVer}` 带版本号的长度
- **用途**：避免每次sync都查询存储节点

### 7.2 优化条件

| **条件**                              | **动作**        |
|---------------------------------------|-----------------|
| `currLen >= hintLen` 且版本匹配        | 跳过查询        |
| `hintLen > currLen` 且版本匹配         | 使用hint        |
| 版本不匹配或无hint                     | 查询存储        |
| truncate操作后                         | 强制查询        |

## 8. 批量处理机制

```mermaid
sequenceDiagram
    participant C1 as "Client1"
    participant C2 as "Client2"
    participant Meta as "MetaServer"
    participant Batch as "BatchedOp"
    
    C1->>Meta: **SyncReq(inode=100)**
    C2->>Meta: **SyncReq(inode=100)**
    
    Meta->>Batch: **创建/获取BatchedOp(100)**
    Meta->>Batch: **添加sync waiter**
    
    Batch->>Batch: **等待批次超时或满**
    
    Batch->>Batch: **合并所有sync请求**
    
    Batch->>Batch: **执行一次queryLength**
    
    Batch->>Batch: **更新inode**
    
    Batch-->>C1: **返回结果**
    Batch-->>C2: **返回结果**
    
    rect rgb(255, 250, 205)
    Note over C1,Batch: **多个sync请求合并为一次元数据操作**
    end
```

## 9. 性能特点

| **特性**           | **实现方式**           | **性能影响**            |
|--------------------|------------------------|------------------------|
| hintLength优化     | 客户端缓存长度提示      | 减少存储查询           |
| 批量处理           | BatchedOp合并请求       | 减少FDB事务           |
| 版本号检查         | syncver/writever比较   | 避免无效同步           |
| 请求转发           | distributor分片        | 负载均衡               |
| 后台周期同步       | periodicSync定时器      | 延迟元数据更新         |

## 10. 关键配置项

| **配置项**                    | **说明**                          |
|-------------------------------|-----------------------------------|
| fsync_length_hint             | fsync是否使用length hint          |
| fdatasync_update_length       | fdatasync是否更新文件长度         |
| ignore_length_hint            | 是否忽略length hint强制查询       |
| time_granularity              | 时间戳精度                        |
| flush_on_stat                 | stat时是否刷新缓冲                |

