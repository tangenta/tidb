# 查询用户历史登录记录

本文档将展示如何查询 TiDB 中用户的历史登录记录。

### 打开关于用户登录信息的控制开关

全局系统变量 `tidb_enable_login_history` 是用来控制用户登录记录的开关。此变量默认值为 `OFF`，表示不记录用户的登录信息，在使用本功能前需要打开此开关。

有关此开关的具体操作如下：
- 查询此变量开关的值
```
show variables like 'tidb_enable_login_history';
```

- 打开此功能
```
set global tidb_enable_login_history='ON';
```

- 关闭此功能
```
set global tidb_enable_login_history='OFF';
```

### 用户历史登录记录系统表

为保障数据库服务的安全，TiDB 支持查询用户的历史登录信息。

用户的历史登录记录保存在系统表 `mysql.login_history` 中，其中包括用户登录时的 User，Connection_id，time，client host 以及登录是否成功等信息，具体 schema 信息如下：

```
login_history | CREATE TABLE `login_history` (
  `Time` timestamp DEFAULT CURRENT_TIMESTAMP,
  `Host` char(255) NOT NULL DEFAULT '',
  `User` char(32) NOT NULL DEFAULT '',
  `DB` char(64) NOT NULL DEFAULT '',
  `Connection_id` bigint(21) NOT NULL DEFAULT '0',
  `Result` char(16) NOT NULL DEFAULT '',
  `Client_host` char(255) NOT NULL DEFAULT '',
  `Detail` text DEFAULT NULL,
  KEY `idx_user` (`User`),
  UNIQUE KEY `idx_session_id` (`Connection_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![ttl] TTL=`time` + INTERVAL 3 MONTH */ /*T![ttl] TTL_ENABLE='ON' */ |
```

### 查询所有用户的登录历史信息

当具备 mysql 系统表的查询权限时，可查询所有用户的历史登录记录。

```
select * from mysql.login_history;
```
通过查询此系统表，用户可通过 sql 筛查某一用户的最近一次的成功或失败的登录记录。

### 查询用户自身的登录历史信息

当用户不具备查询系统表 mysql.login_history 的权限时，可以查询本用户自身的历史登录记录。

```
SELECT * FROM information_schema.user_login_history;
```

### 登录历史记录的自动回收
系统表 mysql.login_history 设置了 TTL，在默认条件下 mysql.login_history 保留 3 个月之内的历史记录。用户可以通过修改 TTL 参数来配置历史登录记录的保留周期，详细操作见[使用 TTL 定期删除过期数据](https://docs.pingcap.com/zh/tidb/v7.0/time-to-live)。