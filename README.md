# 数据仓库课程项目后端

## 基本信息

本后端基于 `Flask` 开发，运行请输入：

```shell
python backend.py
```

## 所需配置

请在使用本后端前将代码开头和结尾如下部分改为你数据库的配置：

```python
# 连接neo4j
graph = Graph('bolt://{your_neo4j_ip}:{your_neo4j_port}', username='neo4j',
              password='{your_neo4j_password}')  # 用户名只能是neo4j，密码是自己定的密码

# 连接Hive；Hive默认没有用户名和密码，输入任意非空用户名和密码均可连接
conn = connect(host='{your_hive_ip}', port=int("{your_hive_port}"), database='default',
               user='{your_hive_user}', password='{your_hive_password}', auth_mechanism='PLAIN')
cur = conn.cursor()

# 连接mysql
db = pymysql.connect(
    host='{your_mysql_ip}',
    port=int("{your_mysql_port}"),
    user='{your_mysql_user}',
    passwd='{your_mysql_password}',
    db='{your_mysql_database}',
)
```

正式运行代码：

```python
server = pywsgi.WSGIServer(("0.0.0.0", int("{your_server_port}")), app)
server.serve_forever()
```

调试运行代码（不稳定，很容易崩溃）：

```python
app.run(host='0.0.0.0', port=12581, debug=True)
```
