---
title: pipeline
markmap:
  colorFreezeLevel: 2
---

## [接口](../../be/src/pipeline/exec/operator.h)

### OperatorBase
- is_sink
- is_source
- open
- close
- is_closed

### OperatorXBase
- get_block
- need_more_input_data
- open
- close
- is_closed

## 名词
### breaking 算子
- 需要把所有的数据都收集齐之后才能运算的算子
### LocalState
- Operator 的内部状态，比如 joinbuild 的 hashtable
### Dependency
- 两个 pipeline 之间的依赖关系。当 upstream pipeline 运行完毕后，
会调用 Dependency 的 set_ready 方法通知 downstream pipeline 执行
