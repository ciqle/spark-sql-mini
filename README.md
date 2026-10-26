# Spark SQL Mini

一个独立的 Scala 学习项目：只在 Spark SQL 外面增加两种写法，用最少的层次看清“自定义语句如何接到 Spark”。

```sql
CREATE TABLE output (id BIGINT) USING PARQUET;
temp = SELECT id FROM range(5) WHERE id >= 2;
INSERT temp INTO TABLE output;
SELECT * FROM output ORDER BY id;
```

## 运行

需要 **JDK 17、Maven 3.9+**。依赖固定为 **Spark 4.2.0、Scala 2.13.18**，首次构建会下载 Spark 依赖，不需要另装 Spark、Hadoop 或 Hive 服务。

```bash
cd ~/workspace/spark-sql-mini
./bin/build test
./bin/run examples/minimal.sql
```

示例最终写入并显示两行：

```text
+---+------+
|id |label |
+---+------+
|2  |item-2|
|3  |item-3|
+---+------+
```

运行脚本使用 `local[2]`，并打印本次 `target/runs/run-.../warehouse` 路径；Parquet 数据保留在这里便于观察。表目录使用 Spark 默认的内存目录，进程结束后表定义和临时视图都会消失。每次演示使用新的目录，重复运行不会向上一次演示的数据追加。需要清理时可以删除 `target/`。

`bin/build` 和 `bin/run` 只在当前进程选择 Java：macOS 在未设置 `JAVA_HOME` 时选 JDK 17；已有 `JAVA_HOME` 时使用你指定的版本。不会修改全局配置。`config/jvm.options` 提供直接嵌入 Spark 所需的 JVM 模块访问选项。

## 两种包装语句的精确含义

| 输入 | 实际交给 Spark 的操作 |
| --- | --- |
| `temp = SELECT ...` | `spark.sql(query).createOrReplaceTempView("temp")` |
| `INSERT temp INTO TABLE db.output` | `spark.sql("INSERT INTO db.output SELECT * FROM temp")` |
| 普通 `CREATE TABLE`、`SELECT`、`EXPLAIN` 等 | `spark.sql(originalSql)` |

首版的赋值右侧只接受 `SELECT` 开头的查询。包装层只识别外部形式，查询内容仍由 Spark 校验；普通 `WITH ... SELECT ...` 可以直接作为原生 SQL 执行。临时视图名和写入源名使用简单英文标识符，如 `temp_1`；目标允许 `output`、`db.output` 等点分名称。普通 SQL 中的标识符、表达式和函数遵循 Spark 本身的规则。

- 同名赋值会替换当前会话中的临时视图；注册视图本身不调用 `show/count/collect`，不自动缓存或物化结果。
- 包装的 `INSERT` 向**已有表追加**，不会自动建表或覆盖旧数据。列按位置匹配，名称不同不会自动重排；列数、类型、权限等问题交给 Spark 报错。
- 多条语句在同一会话中按书写顺序执行，遇到错误立即停止。整个脚本不是事务，前面成功的操作不会回滚。
- 以分号分句，识别单/双引号、反引号、转义、行注释和嵌套块注释；保留 `/*+ ... */` 优化提示。暂不支持包含内部语句分号的 `BEGIN ... END` 等 Spark SQL 脚本块。
- CLI 只展示普通 SQL 的结果；赋值仅打印注册信息，让“建立视图”和“真正求值”在演示中分开可见。

## 先读哪几个文件

1. [Statement.scala](src/main/scala/dev/sparksqlmini/Statement.scala)：三个小类型，描述赋值、写入和普通 SQL。
2. [MiniSql.scala](src/main/scala/dev/sparksqlmini/MiniSql.scala)：核心解释器，直接对照上面的语义表阅读。
3. [StatementParser.scala](src/main/scala/dev/sparksqlmini/StatementParser.scala)：只识别包装语法，不解析 SELECT 内部；分句器避免把字符串中的分号误当分隔符。
4. [Main.scala](src/main/scala/dev/sparksqlmini/Main.scala)：启动本地 Spark、依次执行、展示结果、关闭会话。

```mermaid
flowchart LR
    A[脚本] --> B[识别包装语句与分句]
    B --> C[Statement]
    C --> D[MiniSql.execute]
    D --> E[Spark SQL / 临时视图 API]
    E --> F[Spark 规划与执行]
```

详细跟读见 [从一段脚本走到 Spark](docs/walkthrough.md)。这里没有预先实现 PiT、模型评分、恢复、缓存调度、Catalyst 扩展或多环境适配。

## 测试

```bash
./bin/build test
```

解析测试覆盖分句、注释、转义、原生 SQL 透传与不完整输入。集成测试启动真实本地 Spark，验证 Parquet 落表、追加语义、临时视图替换与会话范围、惰性求值、缺表/不匹配 schema 和错误中断。

在自己的 Scala 程序中，可传入已有会话使用同一个解释器：

```scala
val mini = new MiniSql(spark)
mini.run("temp = SELECT 1L AS id; INSERT temp INTO TABLE output;")
spark.table("output").show()
```

解释器不持有全局 SparkSession，也不负责关闭调用方传入的会话；CLI 才负责会话生命周期。

## 官方依据

- [Spark 下载与 Maven 坐标](https://spark.apache.org/downloads.html)
- [Spark 4.2.0 运行环境](https://spark.apache.org/docs/4.2.0/)
- [Dataset、临时视图与惰性求值](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html)
- [INSERT TABLE 的原生语义](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-dml-insert-table.html)
- [Spark JavaModuleOptions](https://github.com/apache/spark/blob/v4.2.0/launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java)
