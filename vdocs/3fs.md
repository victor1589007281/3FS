# 3FS 分布式文件系统架构总结

## 1. 系统概述

**Fire-Flyer File System (3FS)** 是一个为AI训练和推理工作负载设计的高性能分布式文件系统。它利用现代SSD和RDMA网络提供共享存储层，简化分布式应用的开发。

### 核心特性
- **分离式架构**: 结合数千块SSD的吞吐量和数百个存储节点的网络带宽
- **强一致性**: 实现CRAQ（Chain Replication with Apportioned Queries）
- **文件接口**: 提供标准POSIX文件接口，易于使用
- **高性能**: 在180节点集群上达到6.6 TiB/s的读吞吐量

## 2. 系统架构

### 2.1 架构图

```mermaid
graph TB
    subgraph "**客户端层**"
        style Client1 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        style Client2 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        Client1["**FUSE客户端**<br/>标准POSIX接口"]
        Client2["**Native客户端**<br/>零拷贝异步I/O"]
    end
    
    subgraph "**元数据层**"
        style Meta1 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Meta2 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Meta3 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        Meta1["**Meta Service 1**<br/>无状态服务"]
        Meta2["**Meta Service 2**<br/>无状态服务"]
        Meta3["**Meta Service N**<br/>无状态服务"]
    end
    
    subgraph "**管理层**"
        style Mgmtd1 fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
        style Mgmtd2 fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
        Mgmtd1["**Mgmtd Primary**<br/>主管理节点"]
        Mgmtd2["**Mgmtd Standby**<br/>备用管理节点"]
    end
    
    subgraph "**存储层**"
        style Storage1 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        style Storage2 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        style Storage3 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        Storage1["**Storage Node 1**<br/>CRAQ Chain<br/>多块NVMe SSD"]
        Storage2["**Storage Node 2**<br/>CRAQ Chain<br/>多块NVMe SSD"]
        Storage3["**Storage Node N**<br/>CRAQ Chain<br/>多块NVMe SSD"]
    end
    
    subgraph "**KV存储层**"
        style FDB fill:#ffebee,stroke:#b71c1c,stroke-width:2px
        FDB["**FoundationDB集群**<br/>元数据持久化<br/>SSI事务保证"]
    end
    
    Client1 -->|"**文件操作**"| Meta1
    Client1 -->|"**文件操作**"| Meta2
    Client2 -->|"**文件操作**"| Meta3
    
    Client1 -->|"**数据I/O<br/>RDMA**"| Storage1
    Client2 -->|"**数据I/O<br/>RDMA**"| Storage2
    Client2 -->|"**数据I/O<br/>RDMA**"| Storage3
    
    Meta1 -->|"**元数据读写**"| FDB
    Meta2 -->|"**元数据读写**"| FDB
    Meta3 -->|"**元数据读写**"| FDB
    
    Meta1 -.->|"**查询文件长度**"| Storage1
    Meta2 -.->|"**查询文件长度**"| Storage2
    Meta3 -.->|"**查询文件长度**"| Storage3
    
    Storage1 -->|"**心跳**"| Mgmtd1
    Storage2 -->|"**心跳**"| Mgmtd1
    Storage3 -->|"**心跳**"| Mgmtd1
    
    Meta1 -->|"**心跳**"| Mgmtd1
    Meta2 -->|"**心跳**"| Mgmtd1
    Meta3 -->|"**心跳**"| Mgmtd1
    
    Mgmtd1 -.->|"**主备切换**"| Mgmtd2
    
    Storage1 <-->|"**CRAQ复制**"| Storage2
    Storage2 <-->|"**CRAQ复制**"| Storage3
```

### 2.2 核心模块

#### **Mgmtd (集群管理服务)**
- **职责**:
  - 管理集群成员关系（存储节点、元数据节点）
  - 检测节点故障（基于心跳机制）
  - 分发集群配置和Chain Table
  - 主备切换（通过选举机制）
- **高可用**: 多个Mgmtd实例，其中一个为Primary

#### **Meta Service (元数据服务)**
- **职责**:
  - 处理文件系统元数据操作（create, open, stat, remove等）
  - 管理文件和目录的inode
  - 管理文件会话（write模式打开的文件）
  - 更新文件长度信息
- **特点**:
  - **无状态设计**: 所有状态存储在FoundationDB
  - **水平扩展**: 客户端可连接任意Meta服务
  - **事务保证**: 利用FoundationDB的SSI事务

#### **Storage Service (存储服务)**
- **职责**:
  - 管理本地NVMe SSD
  - 实现CRAQ协议进行数据复制
  - 处理客户端的读写请求
  - 数据恢复和同步
- **关键组件**:
  - **ChunkEngine**: Rust实现的块存储引擎
  - **StorageOperator**: 处理读写请求
  - **ReliableForwarding**: 可靠的链式转发
  - **AioReadWorker**: 异步I/O工作线程

#### **Client (客户端)**
- **FUSE客户端**:
  - 通过FUSE内核模块提供POSIX接口
  - 适用于大多数应用
  - 存在性能限制（内存拷贝、锁竞争）
  
- **Native客户端**:
  - 提供零拷贝异步I/O接口
  - 适用于性能关键型应用
  - 通过共享内存和IoRing实现高性能

## 3. 核心原理

### 3.1 CRAQ复制协议

```mermaid
sequenceDiagram
    participant **客户端** as Client
    participant **Head节点** as Head
    participant **中间节点** as Middle
    participant **Tail节点** as Tail
    
    rect rgb(230, 245, 255)
    Note over Client,Tail: **写操作流程**
    Client->>+Head: **1. Write请求**
    Note over Head: **2. 获取锁**<br/>**写入Pending版本**
    Head->>+Middle: **3. 转发Write**
    Note over Middle: **写入Pending版本**
    Middle->>+Tail: **4. 转发Write**
    Note over Tail: **5. 提交为Committed版本**
    Tail-->>-Middle: **6. ACK**
    Note over Middle: **提交Pending为Committed**
    Middle-->>-Head: **7. ACK**
    Note over Head: **提交Pending为Committed**<br/>**释放锁**
    Head-->>-Client: **8. 返回成功**
    end
    
    rect rgb(255, 243, 224)
    Note over Client,Tail: **读操作流程（任意节点）**
    Client->>+Middle: **1. Read请求**
    alt **只有Committed版本**
        Middle-->>Client: **2. 返回Committed数据**
    else **存在Pending版本**
        Middle-->>Client: **3. 返回特殊状态码**
        Note over Client: **客户端可以重试**<br/>**或请求Pending版本**
    end
    deactivate Middle
    end
```

**CRAQ特点**:
- **Write-All-Read-Any**: 写入所有副本，可从任意副本读取
- **强一致性**: 通过版本号和ACK机制保证
- **负载均衡**: 读请求分散到所有副本
- **故障恢复**: 节点故障时重新配置Chain

### 3.2 数据布局

```mermaid
graph LR
    subgraph "**文件分块**"
        style File fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        File["**File (inode:123)**<br/>**ChunkSize: 4MB**<br/>**StripeSize: 200**"]
    end
    
    subgraph "**Chunk分布**"
        style Chunk0 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Chunk1 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Chunk2 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style ChunkN fill:#fff3e0,stroke:#e65100,stroke-width:2px
        Chunk0["**Chunk 0**<br/>ChunkId: 123-0<br/>Chain: 15"]
        Chunk1["**Chunk 1**<br/>ChunkId: 123-1<br/>Chain: 27"]
        Chunk2["**Chunk 2**<br/>ChunkId: 123-2<br/>Chain: 39"]
        ChunkN["**Chunk N**<br/>ChunkId: 123-N<br/>Chain: X"]
    end
    
    subgraph "**Chain Table**"
        style Chain15 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        style Chain27 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        style Chain39 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        Chain15["**Chain 15**<br/>Node A → Node B → Node C"]
        Chain27["**Chain 27**<br/>Node D → Node E → Node F"]
        Chain39["**Chain 39**<br/>Node B → Node C → Node D"]
    end
    
    File --> Chunk0
    File --> Chunk1
    File --> Chunk2
    File --> ChunkN
    
    Chunk0 --> Chain15
    Chunk1 --> Chain27
    Chunk2 --> Chain39
```

**关键概念**:
- **ChunkSize**: 每个Chunk的大小（如4MB）
- **StripeSize**: 文件条带化的Chain数量（如200）
- **ChunkID**: 由inode ID和chunk索引组成
- **Chain**: 一组Storage Target的复制链

### 3.3 元数据存储

```mermaid
graph TB
    subgraph "**FoundationDB KV Store**"
        style INode fill:#ffebee,stroke:#b71c1c,stroke-width:2px
        style DirEnt fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
        style Session fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        
        INode["**Inode Table**<br/>**Key: INOD + InodeID**<br/>**Value: Inode数据**<br/>- 文件属性<br/>- Layout信息<br/>- 时间戳"]
        
        DirEnt["**Directory Entry**<br/>**Key: DENT + ParentID + Name**<br/>**Value: 目标Inode信息**<br/>- InodeID<br/>- 类型"]
        
        Session["**File Session**<br/>**Key: SESS + InodeID + SessionID**<br/>**Value: 会话信息**<br/>- ClientID<br/>- 时间戳"]
    end
    
    subgraph "**Meta操作**"
        style TxRead fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        style TxWrite fill:#fff3e0,stroke:#e65100,stroke-width:2px
        TxRead["**只读事务**<br/>stat, lookup, listdir"]
        TxWrite["**读写事务**<br/>create, link, unlink, rename"]
    end
    
    TxRead -.->|"**查询**"| INode
    TxRead -.->|"**查询**"| DirEnt
    TxWrite -->|"**修改**"| INode
    TxWrite -->|"**修改**"| DirEnt
    TxWrite -->|"**修改**"| Session
```

**元数据操作特点**:
- **事务保证**: 利用FoundationDB的SSI（Serializable Snapshot Isolation）
- **冲突检测**: 并发事务冲突时自动重试
- **范围查询**: 高效的目录列表操作
- **增量Inode ID**: 单调递增的全局唯一ID

## 4. 关键交互时序

### 4.1 文件创建流程

```mermaid
sequenceDiagram
    participant **Client** as Client
    participant **Meta** as Meta Service
    participant **FDB** as FoundationDB
    participant **Mgmtd** as Mgmtd
    
    rect rgb(230, 245, 255)
    Note over Client,Mgmtd: **创建文件流程**
    Client->>+Meta: **1. Create(path, mode)**
    
    Meta->>+FDB: **2. 开始事务**
    Meta->>FDB: **3. 查询父目录Inode**
    FDB-->>Meta: **4. 返回父目录信息**
    
    Meta->>Meta: **5. 生成新InodeID**
    Meta->>Meta: **6. 选择Chain（Round-Robin）**
    Meta->>Meta: **7. 生成Shuffle Seed**
    
    Meta->>FDB: **8. 写入新Inode**
    Meta->>FDB: **9. 写入DirEntry**
    Meta->>FDB: **10. 提交事务**
    FDB-->>Meta: **11. 事务成功**
    deactivate FDB
    
    Meta-->>-Client: **12. 返回文件信息**
    end
```

### 4.2 文件读取流程

```mermaid
sequenceDiagram
    participant **Client** as Client
    participant **Meta** as Meta Service
    participant **Storage1** as Storage Node 1
    participant **Storage2** as Storage Node 2
    
    rect rgb(255, 243, 224)
    Note over Client,Storage2: **文件读取流程**
    Client->>+Meta: **1. Open(path, O_RDONLY)**
    Meta->>Meta: **2. 查询Inode和Layout**
    Meta-->>-Client: **3. 返回文件元数据**
    
    Client->>Client: **4. 计算Chunk位置**
    
    par **并行读取多个Chunk**
        Client->>+Storage1: **5a. BatchRead(Chunk0,1,2)**
        Note over Storage1: **从本地SSD读取**
        Storage1-->>-Client: **6a. 返回数据**
    and
        Client->>+Storage2: **5b. BatchRead(Chunk3,4,5)**
        Note over Storage2: **从本地SSD读取**
        Storage2-->>-Client: **6b. 返回数据**
    end
    
    Client->>Client: **7. 组装完整数据**
    end
```

### 4.3 文件写入流程

```mermaid
sequenceDiagram
    participant **Client** as Client
    participant **Meta** as Meta Service
    participant **Head** as Storage Head
    participant **Middle** as Storage Middle
    participant **Tail** as Storage Tail
    
    rect rgb(232, 245, 233)
    Note over Client,Tail: **文件写入流程**
    Client->>+Meta: **1. Open(path, O_WRONLY)**
    Meta->>Meta: **2. 创建FileSession**
    Meta-->>-Client: **3. 返回SessionID**
    
    Client->>+Head: **4. Write(ChunkID, offset, data)**
    Note over Head: **5. RDMA Read获取数据**
    Note over Head: **6. 获取Chunk锁**
    Note over Head: **7. 写入Pending版本**
    
    Head->>+Middle: **8. Update(全量Chunk)**
    Note over Middle: **写入Pending版本**
    
    Middle->>+Tail: **9. Update(全量Chunk)**
    Note over Tail: **10. 提交为Committed**
    Tail-->>-Middle: **11. ACK**
    Note over Middle: **提交为Committed**
    
    Middle-->>-Head: **12. ACK**
    Note over Head: **13. 提交为Committed**
    Note over Head: **14. 释放锁**
    
    Head-->>-Client: **15. Write成功**
    
    Client->>+Meta: **16. Close(fd)**
    Meta->>Meta: **17. 查询精确文件长度**
    Meta->>Meta: **18. 删除FileSession**
    Meta-->>-Client: **19. Close成功**
    end
```

## 5. 使用场景

### 5.1 AI训练场景

```mermaid
graph LR
    subgraph "**训练集群**"
        style GPU1 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        style GPU2 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        style GPU3 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        GPU1["**GPU节点1**<br/>DataLoader"]
        GPU2["**GPU节点2**<br/>DataLoader"]
        GPU3["**GPU节点N**<br/>DataLoader"]
    end
    
    subgraph "**3FS存储**"
        style Dataset fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Checkpoint fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        Dataset["**训练数据集**<br/>- 随机访问<br/>- 无需预取<br/>- 高吞吐"]
        Checkpoint["**模型Checkpoint**<br/>- 并行写入<br/>- 大文件<br/>- 高带宽"]
    end
    
    GPU1 -->|"**随机读取样本**"| Dataset
    GPU2 -->|"**随机读取样本**"| Dataset
    GPU3 -->|"**随机读取样本**"| Dataset
    
    GPU1 -->|"**保存检查点**"| Checkpoint
    GPU2 -->|"**保存检查点**"| Checkpoint
```

**优势**:
- **随机访问**: 无需数据预取或shuffle
- **高吞吐**: 充分利用SSD和RDMA带宽
- **并行Checkpoint**: 支持大规模并行写入

### 5.2 数据准备场景

```mermaid
graph TB
    subgraph "**数据处理流水线**"
        style Raw fill:#ffebee,stroke:#b71c1c,stroke-width:2px
        style Process fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
        style Output fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
        Raw["**原始数据**"]
        Process["**处理任务**<br/>（并行）"]
        Output["**输出数据集**"]
    end
    
    subgraph "**3FS特性**"
        style Feature1 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        style Feature2 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        Feature1["**目录结构**<br/>- 层次化组织<br/>- 原子rename<br/>- 递归删除"]
        Feature2["**小文件管理**<br/>- 高效存储<br/>- 快速清理"]
    end
    
    Raw --> Process
    Process --> Output
    Output -.-> Feature1
    Output -.-> Feature2
```

### 5.3 推理KV Cache场景

```mermaid
graph TB
    subgraph "**推理服务**"
        style Infer1 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        style Infer2 fill:#e1f5ff,stroke:#01579b,stroke-width:2px
        Infer1["**推理节点1**"]
        Infer2["**推理节点N**"]
    end
    
    subgraph "**KV Cache in 3FS**"
        style Cache fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style GC fill:#ffebee,stroke:#b71c1c,stroke-width:2px
        Cache["**KV Cache文件**<br/>- 大容量<br/>- 高吞吐读取<br/>- 成本低于DRAM"]
        GC["**垃圾回收**<br/>- 自动清理<br/>- 高IOPS"]
    end
    
    Infer1 <-->|"**读写KV**"| Cache
    Infer2 <-->|"**读写KV**"| Cache
    Cache --> GC
```

**优势**:
- **大容量**: SSD容量远超DRAM
- **成本效益**: 比DRAM缓存更经济
- **高性能**: 40 GiB/s峰值吞吐

## 6. 与其他文件系统对比

### 6.1 功能对比表

| **特性** | **3FS** | **Lustre** | **CephFS** | **HDFS** |
|---------|---------|-----------|-----------|---------|
| **架构** | 分离式架构，无状态Meta | 集中式元数据 | 分布式元数据 | NameNode + DataNode |
| **一致性** | 强一致性（CRAQ） | 强一致性 | 最终一致性→强一致性 | 强一致性 |
| **元数据存储** | FoundationDB | 单点或HA | Rados（分布式） | NameNode内存+日志 |
| **复制协议** | CRAQ（读任意副本） | 客户端选择 | 主副本 | Pipeline写 |
| **网络** | RDMA优化 | TCP/RDMA | TCP | TCP |
| **小文件性能** | 优秀（事务+批量） | 中等 | 较差 | 较差 |
| **随机读性能** | 极高（RDMA+SSD） | 高 | 中等 | 差 |
| **扩展性** | 水平扩展（无状态） | 受限于元数据服务器 | 水平扩展 | 水平扩展 |
| **接口** | POSIX + Native API | POSIX | POSIX | HDFS API |

### 6.2 性能对比

```mermaid
graph TB
    subgraph "**性能维度对比**"
        style Perf1 fill:#e8f5e9,stroke:#1b5e20,stroke-width:3px
        style Perf2 fill:#e8f5e9,stroke:#1b5e20,stroke-width:3px
        style Perf3 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Perf4 fill:#fff3e0,stroke:#e65100,stroke-width:2px
        style Perf5 fill:#ffebee,stroke:#b71c1c,stroke-width:2px
        
        Perf1["**峰值吞吐（大块顺序读）**<br/>3FS: 6.6 TiB/s ⭐⭐⭐⭐⭐"]
        Perf2["**随机读IOPS**<br/>3FS: 极高（RDMA直连） ⭐⭐⭐⭐⭐"]
        Perf3["**元数据操作**<br/>3FS: 高（FDB事务） ⭐⭐⭐⭐"]
        Perf4["**小文件处理**<br/>3FS: 优秀 ⭐⭐⭐⭐"]
        Perf5["**单文件写入**<br/>3FS: 中等（FUSE限制） ⭐⭐⭐"]
    end
```

### 6.3 架构差异

**3FS的独特设计**:
1. **无状态元数据服务**: 利用FoundationDB作为唯一的状态存储，Meta服务可随意扩展
2. **CRAQ优化**: Write-All-Read-Any，最大化读取吞吐量
3. **Native Client API**: 零拷贝异步I/O，突破FUSE性能瓶颈
4. **AI工作负载优化**: 针对训练和推理场景的特定优化

**传统文件系统限制**:
- **Lustre**: 元数据服务器是瓶颈
- **CephFS**: 复杂的元数据分片，小文件性能较差
- **HDFS**: 不支持POSIX，NameNode内存限制

## 7. 总结

### 7.1 核心优势

```mermaid
mindmap
  root((**3FS核心优势**))
    **高性能**
      RDMA网络
      全闪存SSD
      并行访问
      零拷贝I/O
    **强一致性**
      CRAQ协议
      FDB事务
      原子操作
    **易用性**
      POSIX接口
      目录结构
      熟悉的API
    **可扩展**
      无状态Meta
      水平扩展
      负载均衡
    **AI优化**
      随机访问
      并行Checkpoint
      KV Cache
```

### 7.2 技术亮点

1. **分离式架构**: 计算、元数据、存储完全解耦，独立扩展
2. **CRAQ复制**: 平衡一致性和性能，读取可从任意副本
3. **FoundationDB支撑**: 强事务保证，简化元数据管理
4. **零拷贝I/O**: Native API突破FUSE限制，达到极致性能
5. **RDMA深度集成**: 充分利用现代网络硬件

### 7.3 适用场景

| **场景** | **适用度** | **原因** |
|---------|----------|---------|
| **大规模AI训练** | ⭐⭐⭐⭐⭐ | 随机访问、高吞吐、并行Checkpoint |
| **LLM推理KV Cache** | ⭐⭐⭐⭐⭐ | 大容量、高性能、低成本 |
| **数据预处理** | ⭐⭐⭐⭐⭐ | 目录操作、小文件管理 |
| **通用文件存储** | ⭐⭐⭐⭐ | POSIX兼容、易用 |
| **单文件大规模写** | ⭐⭐⭐ | FUSE写性能限制（可用Native API） |

---

## 附录：关键技术指标

- **集群规模**: 180节点存储集群
- **峰值吞吐**: 6.6 TiB/s（大块读）
- **网络**: 200/400 Gbps InfiniBand
- **存储**: 16×14TB NVMe SSD/节点
- **副本数**: 通常3副本（可配置）
- **ChunkSize**: 可配置（典型4MB）
- **元数据后端**: FoundationDB 7.1+

