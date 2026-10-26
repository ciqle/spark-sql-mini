# 从一段脚本走到 Spark

先打开 `examples/minimal.sql`，再对照 `MiniSql.scala`。第一阶段只理解一件事：包装层把自己的少量语法映射为 Spark 已有能力。

## 1. 识别外部结构

```sql
temp = SELECT id FROM range(5) WHERE id >= 2;
```

解析器只识别左边的名称和等号，生成：

```scala
AssignTemp("temp", "SELECT id FROM range(5) WHERE id >= 2")
```

它不知道 `range`、`WHERE` 或列类型是什么意思。这些属于 Spark SQL 的职责。`Statement` 是本项目自己的简单数据类型，并非 Catalyst 逻辑计划。

分句稍有不同：`SELECT 'a;b'` 中的分号属于字符串，因此不能直接 `split(";")`。解析文件里的扫描循环只负责字符串、注释和语句边界；它不是完整 SQL 解析器。

## 2. 在 Driver 注册临时视图

解释器对赋值只做三步：

```scala
val data = spark.sql(query)
data.createOrReplaceTempView(name)
data
```

Spark 返回 DataFrame，并把它注册在当前会话的目录里。后续 `SELECT * FROM temp` 可以继续使用它。这里没有把所有结果收集到本地，也没有主动缓存。

DataFrame 承载查询计划。建立计划与注册视图，不等于整份数据已经算完。查询分析、元数据处理仍可能立即发生；真正的行计算通常由 `show`、`collect`、写出等动作触发。集成测试通过累加器观察这个区别。

## 3. 把写入包装改写成原生 SQL

```sql
INSERT temp INTO TABLE output;
```

解释器把它转成：

```sql
INSERT INTO output SELECT * FROM temp
```

之后调用 `spark.sql(...)`。目标表必须已经存在，这条语句采用 Spark 原生的追加与按位置匹配列的规则。存储格式、类型检查、查询优化、分区任务和实际写出都由 Spark 完成。

这里选择原生 SQL 改写，是为了让写入路径可以直接对照 Spark 的官方语法。首版没有额外的写入框架。

## 4. 普通 SQL 保持可用

`CREATE TABLE`、`SELECT` 和 `EXPLAIN` 生成 `SparkSql`，交给相同 SparkSession。这样演示脚本能够自建目标表、写入并查询验证，而不需要包装层再实现建表语言。

CLI 对普通查询调用 `show` 显示结果；对赋值不调用 `show`。运行日志中先看到注册临时视图，再看到写入完成，最后看到输出行。

## 5. 用现成的计划工具观察

示例最后包含：

```sql
EXPLAIN FORMATTED SELECT * FROM selected;
```

观察输出中的 `Range`、`Filter`、`Project`。这是 Spark 对查询的理解。包装语法已在进入 Spark 前被解释，所以计划里不会凭空出现 `AssignTemp` 或本项目自定义的物理算子。

还可以把示例中的 `INSERT` 写两次：同一次运行中，输出会多出相同的两行，这是追加语义。把第二次写入目标改成不存在的表，则 Spark 报错并停止，之前成功的写入仍保留。

## 6. 当前边界

控制逻辑在 Driver 调用 Spark；数据行计算由 Spark Task 执行。此版本没有引入自定义 Spark parser、Catalyst Rule、分布式调度器。

后续增加功能时，可以再逐项讨论它需要属于哪一层。当前保留这条可验证的最短路径，不预先设计尚未需要的扩展体系。
