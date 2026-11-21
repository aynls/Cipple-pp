# BufferReader 与 BufferWriter 的交互

`BufferWriter` 与 `BufferReader` 是一对对称的编解码器（codec），共同定义了 Cipple++ 的线格式（wire
format）。二者不互相引用，唯一的契约是字节缓冲：

- **写侧**：`BufferWriter` 按线格式把消息追加编码进内部 `std::vector<std::uint8_t>`，通过 `bytes()` 交出字节。
- **读侧**：`BufferReader` 以 `std::span<const std::uint8_t>` 只读视图承接这些字节，按相同顺序解码。零拷贝，不持有所有权。

典型交接一行完成：

```cpp
BufferWriter writer;
// ... writer.write_*() 编码 ...
BufferReader reader{writer.bytes()}; // 写侧产物 → 读侧视图
// ... reader.read_*() 解码 ...
```

## 线格式契约

| 字段类型              | 编码                                          | 解码                                       |
|-----------------------|-----------------------------------------------|--------------------------------------------|
| 整数                  | `write_u32/u64` 大端序逐字节移位写出          | `read_u32/u64` 移位重组                    |
| 有符号整数            | `bit_cast` 转无符号后走同一路径               | 对称 `bit_cast` 还原，位级精确             |
| 字符串                | `write_string`：`u64` 长度前缀 + UTF-8 裸字节 | `read_string`：先读长度，校验剩余后拷贝    |
| 定长裸字节（如 uuid） | `write_bytes`，无长度前缀                     | `read_bytes` / `read_fixed<N>`，零拷贝视图 |

纪律：**解码必须严格按编码时的字段顺序调用对应 `read_*`**。Reader 内部只有单调前进的 `_pos` 游标，格式无自描述信息。

## patch_u32：帧长度回填

帧编码的经典用法：写帧头时先占位写一个 u32，编码变长消息体后，用 `patch_u32(pos, size)` 回填实际长度，避免预先计算长度或预留容量。对
reader 完全透明——reader 在帧边界先 `read_u32` 取长度，再决定读取多少字节。

## 不对称之处

- **错误模型**：writer 几乎不失败（仅 `patch_u32` 越界抛 `std::out_of_range`）；reader 是防御方，每个 `read_*` 经
  `require(n)` 检查剩余字节，数据不足即抛异常，可安全处理截断的流式输入。
- **所有权**：writer 拥有字节；reader 借用 `span` 视图，可叠加在 writer 的缓冲之上。
