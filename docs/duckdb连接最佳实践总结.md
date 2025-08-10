1. 黄金法则：每个线程一个连接 (One Connection Per Thread)
这是最重要、必须遵守的规则。由于连接对象非线程安全，任何需要并发访问数据库的线程都必须创建和使用自己独立的连接。

2. 拥抱短生命周期连接 (Embrace Short-Lived Connections)
由于连接创建成本极低，不要吝啬创建和销毁连接。传统的“连接池”模式在 DuckDB 中并非必需品，甚至常常是过度设计。
'''python
# with 语句会在代码块结束时自动关闭连接
with duckdb.connect('my_database.duckdb') as conn:
    conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    df = conn.execute("SELECT * FROM test").fetchdf()
# 此处 conn 已自动关闭
'''

3. 理解并发模型：多读单写
DuckDB 使用 MVCC (Multi-Version Concurrency Control) 实现并发。

多个读线程：可以有任意多个线程同时从数据库中读取数据，它们互不阻塞。每个读线程都应该使用自己的 read_only=True 连接。
单个写线程：在任何一个时间点，只能有一个线程可以执行写操作（INSERT, UPDATE, DELETE, CREATE, ALTER 等）。当一个写事务正在进行时，其他尝试写入的线程会被阻塞，直到写事务提交或回滚。
读写不阻塞：一个正在进行的写事务不会阻塞读线程。读线程会看到写事务开始之前的数据快照。
实践建议：如果你的应用有大量读和少量写，可以设计成多个读线程和一个专门的写线程/队列模式，以最大化并发性能。

4. 连接池：何时需要？
如前所述，大多数情况下你不需要为 DuckDB 设置连接池。

但以下场景可以考虑使用：

Web 应用 (如 Flask/FastAPI)：在一个高并发的 Web 服务器中，你可能希望限制并发到数据库的总连接数，以控制资源使用。这时，一个轻量级的池（比如基于 queue.Queue）或者与 Web 框架集成的连接管理方案会很有用。在 FastAPI 中，你可以使用 Depends 系统来管理每个请求的连接生命周期。
简化线程管理：在复杂的应用中，一个简单的池可以帮助你抽象化 threading.local 的管理逻辑。
但请记住，这个池的主要目的不是为了复用“昂贵”的连接，而是为了管理和限制并发。

5. 明确管理数据库文件路径
使用绝对路径或相对于项目根目录的明确相对路径来连接数据库文件，避免因当前工作目录变化导致找不到文件或创建了多个数据库文件。
善用特殊路径：
:memory:：创建一个纯内存数据库，速度最快，但程序结束时数据会丢失。每个连接默认是独立的内存数据库。
?shared=true (DuckDB 1.0.0+): 通过在文件名后附加 ?shared=true 或使用 :memory:?shared=true，可以让来自同一进程的多个连接共享同一个内存数据库。这在多线程测试或需要共享内存状态时非常有用。

6. 显式管理事务
虽然 DuckDB 默认是自动提交（auto-commit）模式，但对于需要原子性保证的多个操作，一定要使用显式事务。

'''python
with duckdb.connect('my_database.duckdb') as conn:
    try:
        # 开始一个事务
        conn.begin()
        conn.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1;")
        conn.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2;")
        # 如果一切顺利，提交事务
        conn.commit()
    except Exception as e:
        print(f"Transaction failed: {e}. Rolling back.")
        # 如果发生错误，回滚事务
        conn.rollback()
'''

7. 要避免的反模式 (Anti-Patterns)
在多个线程间共享同一个连接对象：绝对禁止！这是导致问题的首要原因。
为每一行插入都打开/关闭连接：在循环中执行 for row in data: with duckdb.connect() as conn: conn.execute("INSERT ...") 效率极低。应该在一个连接和事务内批量插入数据。
忘记关闭连接：虽然 DuckDB 的资源泄漏问题没有网络数据库那么严重，但忘记关闭仍可能导致文件句柄未释放等问题。始终使用 with 语句或 try...finally... 来确保关闭。

总之，DuckDB 的连接管理哲学是**“即用即创，用完即弃”。牢记“每个线程一个连接”**的黄金法则，并善用上下文管理器，你就能编写出健壮、高效的 DuckDB 应用。