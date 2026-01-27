# login history 设计文档

## 摘要
本文档是关于 TiDB 中查询用户登录历史记录这一功能的设计文档。

## 背景
为了满足数据库服务的安全，需要对登录数据库的用户信息做审计，同时也可以查询数据库的登录历史，这也是分布式数据库的安全标准之一。

## 功能需求
当用户登录成功后，通过SQL命令可以查看当前账户的历史登录信息，包括登录成功、登录失败的场景。

（1）【基本要求点】会话建立成功后，可以查询上一次成功建立会话的历史信息（用户、登录数据库、时间、IP等）；

（2）【基本要求点】会话建立成功后，可以查询上一次会话建立未成功的尝试信息（用户、登录数据库、时间、IP等），以及上一次成功建立会话以来的不成功尝试次数；

（3）【基本要求点】 用户可以控制访问历史记录的保存时间，默认值时间长度建议为90天。同时，第1、2点查询结果会被第3点限制。

## 详细设计

### 变量开关控制

- 新增系统变量 `tidb_enable_login_history` 作为记录用户登录信息的控制开关
  - 系统变量 `tidb_enable_login_history` 的作用域为 global scope，修改后支持持久化。
  - 变量的默认值为 `OFF`，表示在默认条件下，用户登录 TiDB 数据库不会留下用户登录记录。当设置变量的值为 `ON` 后，登录数据库时才会留下登录记录。

### 系统表设计

- 添加系统表 mysql.login_history 和视图 INFORMATION_SCHEMA.User_LOGIN_HISTORY
  - 系统表的 schema 如下
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
  INDEX `idx_user` (`User`),
  INDEX `idx_session_id` (`Connection_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![ttl] TTL=`time` + INTERVAL 12 HOUR */ /*T![ttl] TTL_ENABLE='ON' */
  ```

  - 视图的 schema 如下
  ```
  User_LOGIN_HISTORY | CREATE TABLE `User_LOGIN_HISTORY` (
  `TIME` datetime DEFAULT NULL,
  `HOST` varchar(64) DEFAULT NULL,
  `USER` varchar(32) DEFAULT NULL,
  `DB` varchar(64) DEFAULT NULL,
  `CONNECTION_ID` bigint(21) DEFAULT NULL,
  `RESULT` varchar(16) DEFAULT NULL,
  `CLIENT_HOST` varchar(64) DEFAULT NULL,
  `DETAIL` varchar(1024) DEFAULT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  ```

- 用户建立数据库连接时，需要验证登录信息
  - 如果成功，则向 mysql.login_history 插入登录失败的记录
  - 如果登录成功，向 mysql.login_history 插入登录成功的记录
- 当用户 user01 查询 INFORMATION_SCHEMA.User_LOGIN_HISTORY 时，过滤掉不属于 user01 记录即可。