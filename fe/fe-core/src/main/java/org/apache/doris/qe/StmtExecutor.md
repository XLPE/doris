```mermaid
---
title: 整体执行流程
---
graph TD
A[开始查询] --> B[解析SQL]
B --> C[创建Analyzer实例]
C --> D[分析SQL语句]
D --> E[生成执行计划]
E --> F[创建Planner实例]
F --> G[计划查询]
G --> H[创建QueryInfo实例]
H --> I[注册查询]
I --> J[创建Coordinator实例]
J --> K[分配后端任务]
K --> L[通过BackendServiceProxy发送任务]
L --> M[后端执行任务]
M --> N[返回结果]
N --> O[通过MysqlChannel接收结果]
O --> P[创建ResultSet实例]
P --> Q[解析结果]
Q --> R[结束查询]

    %% 添加类之间的关系
    subgraph SQL解析
        B --> C
        C --> D
    end

    subgraph 执行计划生成
        D --> E
        E --> F
        F --> G
    end

    subgraph 查询执行
        G --> H
        H --> I
        I --> J
        J --> K
        K --> L
        L --> M
        M --> N
        N --> O
        O --> P
        P --> Q
        Q --> R
    end

    %% 添加类之间的依赖关系
```

```mermaid
---
title: sql执行流程
---
flowchart TD
subgraph ReadListener
    A[handleEvent] --> B[异步processOnce]
end
subgraph MysqlConnectProcessor
    B --> C[dispatch]
    C --> D[handleExecute 170]
    D --> E[handleExecute 103]
end
subgraph StmtExecutor
    E --> F[execute]
    F --> G[queryRetry]
    G --> I[execute queryId]
    I --> J[executeByNereids]
    J --> K[handleQueryWithRetry]
    K --> L[handleQueryStmt]
    L --> N[executeAndSendResult]
end
```
#### 代码引用：
1. src/main/java/org/apache/doris/qe/StmtExecutor.java
2. src/main/java/org/apache/doris/qe/MysqlConnectProcessor.java
3. src/main/java/org/apache/doris/mysql/ReadListener.java

#### 核心功能位置说明：
1. sql解析在[executeByNereids]调用 parseByNereids
2. logicalPlan是Command类型，在[executeByNereids]调用 ((Command) logicalPlan).run(context, this)
3. logicalPlan是非Command类型(查询)，在[executeByNereids]调用 planner.plan
4. 如果是强一致读取，非master的FE在逻辑计划执行之前会强制同步一次Journal，调用syncJournalIfNeeded
5. 执行计划生成在[executeByNereids]调用 planner.plan
6. explain在[handleQueryStmt]调用 planner.getExplainString
7. 查询执行在[executeAndSendResult]调用 coordBase.exec()
8. 从BE获取结果在[executeAndSendResult]调用 coordBase.getNext()
9. 查询结果发送给客户端在[executeAndSendResult]调用 channel.sendOnePacket
10. 查询缓存在[executeAndSendResult]调用 context.getEnv().getSqlCacheManager().tryAddBeCache
11. 审计日志写入在[handleExecute 103]调用 auditAfterExec






















