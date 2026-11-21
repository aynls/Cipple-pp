# Cipple++

面向集群的 C++23 配置发布订阅系统。项目设计见 `任务书.md` 和 `doc/architecture.md`。

## 约定

- `.hpp`/`.cpp` 成对，文件名 snake_case，命名空间 `cipple::*`，头文件一律 `#pragma once`
- 头文件只放声明；能用前向声明就不用 `#include`；头文件里禁止 `using namespace`
- 基类必须 `virtual` 析构；重写显式标 `override`；getter 一律 `const noexcept`
- 不修改对象成员的成员函数一律标 `const`
- 确认异常不会泄露、或基本不可能抛出异常的函数一律标 `noexcept`
- 消息对象构造后不可变；所有权转移用 `std::unique_ptr`
- 源文件显式列入 CMakeLists（禁用 `file(GLOB)`）
