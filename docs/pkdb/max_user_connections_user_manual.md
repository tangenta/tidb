# 限制用户连接到 TiDB 的 session 个数

本文档主要介绍如何限制 TiDB 中用户的连接数。

## 简介

TiDB 支持用户级别的连接数限制，以防止数据库的恶意连接攻击，保证数据库安全。

## 全局变量 max_user_connections作为用户连接数限制的缺省值

当创建一个用户时，默认使用变量 max_user_connections 作为用户的 session 个数限制，其中：
- max_user_connections 作用域为 global；
-  max_user_connections 的修改范围为 0～100000，值为 0 表示不做限制。

### 用户连接数默认不设限

当新创建用户时，默认将 max_user_connections作为用户的 session 个数限制，而 max_user_connections 的默认值为 0，表示在默认情况下不限制用户级别的 session 个数。

### 修改默认用户连接数

可通过修改 max_user_connections 的值来限制用户连接数
```
 set global max_user_connections = @value;
```

如下面的例子:

```
mysql > set global max_user_connections = 3;
Query OK, 0 rows affected (0.01 sec)
```
表示每个 tidb instance 最多允许同一个 user 建立 3 个连接。

### 查询默认用户连接数

可以通过查询 max_user_connections的值，来确认用户的默认连接个数。如

```
mysql > show variables like 'max_user_connections';
+----------------------+-------+
| Variable_name        | Value |
+----------------------+-------+
| max_user_connections | 0     |
+----------------------+-------+
1 row in set (0.00 sec)
```

上面的 sql 查询到 max_user_connections值为 0，表示默认用户连接数不设限。

## 指定用户的连接数限制

### 为指定用户设置连接数限制

在创建用户时，也可以为指定用户设置连接数限制。
```
create user @user WITH MAX_USER_CONNECTIONS @value;
```

比如：
```
mysql > create user user02 WITH MAX_USER_CONNECTIONS 3;
```

表示创建用户 user02，并且 user02 最大可同时登录 3 个连接到同一个 tidb instance.

### 查询指定用户的连接数限制

可以查询系统表 mysql.user 来查询某个用户的连接数限制。如：

```
mysql > select host, user, max_user_connections from mysql.user where user = "user02";
+------+--------+----------------------+
| host | user   | max_user_connections |
+------+--------+----------------------+
| %    | user02 |                    3 |
+------+--------+----------------------+
1 row in set (0.00 sec)
```

上面的 sql 查询到 user02 的连接数限制为 3，表示可同时创建 3 个连接到同一个 tidb instance.

### 修改指定用户的连接数限制

TiDB 支持修改指定用户的连接数限制。

```
alter user @user WITH MAX_USER_CONNECTIONS @value;
```

如：
```
alter user 'user01' WITH MAX_USER_CONNECTIONS 4;
```
表示修改用户 user01 的连接数限制为 4，此时可同时创建 4 个连接到同一个 tidb instance.

> **注意：**
> 
> - 由于 TiDB 是一个分布式数据库，所以这里用户级别的连接数限制作用在 tidb instance 上；
> - 当指定用户的连接数限制数非 0 时，指定用户的连接数限制数将起作用；
> - 当指定用户的连接数限制数为 0 时，全局变量 max_user_connections 作为缺省值会起作用；
> - 指定用户的连接数限制数和全局变量 max_user_connections 都为 0 时，表示此用户连接不设限。
