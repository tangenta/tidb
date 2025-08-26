# max_user_connections 设计文档

## 摘要
本文档是关于 TiDB 中的限制用户最大连接数这一功能的设计文档。

## 背景
基于国内各项分布式数据库标准，TiDB 需要支持必不可少的安全功能项，其中一项就是会话管理，需要对每一个用户的会话连接数做限制，以防止数据库的恶意连接攻击，保证数据库安全。

## 功能需求
- 需要支持用户级别连接数限制（用户级别最大连接数可设置，全局缺省单个用户最大连接数限制）；
- 兼容Mysql设计、语法
  - 【基本要求点】在CREATE/ALTER USER命令中增加 MAX_USER_CONNECTIONS选项，实现指定用户的最大连接数限制（mysql：https://dev.mysql.com/doc/refman/8.0/en/user-resources.html ）；
  - 【基本要求点】增加系统变量max_user_connections，设置全局级别的单个用户的最大连接数限制（缺省的用户级别最大连接数），设置后持久化，重启不会失效。（Mysql：https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html ）；
  - 【生效范围】按单一节点控制，暂时不做到全局控制。即每个节点控制当前节点的用户最大连接数。——这也是与当前 TiDB 的 max_connections 保持一致；
  - 【优先级】保持与mysql一致，第（1）条的优先级高于第（2）条，即缺省的用户级别最大连接数优先级更低。当没有设置（1），或（1）设置为0时，第（2）条才生效；
  - 【不作为验收条件】设计中需兼顾后续迭代能力，高于合规标准的设计还包括：
    - 兼容mysql的其他资源限制（每小时的用户连接数、每小时的查询数等）
    - 为特权账户保留一定的连接数，在极端情况下可以登录到数据库进行运维操作

## 详细设计

### 全局变量：max_user_connections

添加一个全局变量 max_user_connections 作为用户的缺省限制，具体设计如下：
- 这个 variables 是 global 级别，默认值是 0，表示没有限制；
- 此变量支持在线修改，修改范围限定在 0 ～ 10000，支持持久化；

### 系统表 mysql.user 修改

在系统表 mysql.user中添加一个字段 max_user_connections，来记录每一个用户的连接限制，系统表如下：
```
CREATE TABLE `user` (
  `Host` char(255) NOT NULL,
  `User` char(32) NOT NULL,
  `authentication_string` text DEFAULT NULL,
  `plugin` char(64) DEFAULT NULL,
  `Select_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Insert_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Update_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Delete_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Drop_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Process_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Grant_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `References_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Alter_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Show_db_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Super_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_tmp_table_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Lock_tables_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Execute_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_view_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Show_view_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_routine_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Alter_routine_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Index_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_user_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Event_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Repl_slave_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Repl_client_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Trigger_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_role_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Drop_role_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Account_locked` enum('N','Y') NOT NULL DEFAULT 'N',
  `Shutdown_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Reload_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `FILE_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Config_priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Create_Tablespace_Priv` enum('N','Y') NOT NULL DEFAULT 'N',
  `Password_reuse_history` smallint(5) unsigned DEFAULT NULL,
  `Password_reuse_time` smallint(5) unsigned DEFAULT NULL,
  `User_attributes` json DEFAULT NULL,
  `Token_issuer` varchar(255) DEFAULT NULL,
  `Password_expired` enum('N','Y') NOT NULL DEFAULT 'N',
  `Password_last_changed` timestamp DEFAULT CURRENT_TIMESTAMP,
  `Password_lifetime` smallint(5) unsigned DEFAULT NULL,
  `Max_user_connections` smallint(5) unsigned DEFAULT '0',
  PRIMARY KEY (`Host`,`User`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

- 在创建用户时，如果不指定连接数，默认Max_user_connections为 0 ，表示用户的连接数不做限制；
- 支持在创建用户时指定连接数，如create user user02 WITH MAX_USER_CONNECTIONS 3;
- 支持修改用户连接数，如 alter user 'user01' WITH MAX_USER_CONNECTIONS 4.

### 用户连接数判断

-  tidb Server 中维护了所有登录这个 tidb instance 的 connection 信息，在 Server 中维护一个 map，具体数据结构如下
```
// 位于 Server 中的 map 信息，具体维护的每一个用户的资源限制
userResource map[string]*userResourceLimits
 
type userResourceLimits struct {
    reset_utime   uint64
    connections   int
    conn_per_hour int32
    updates       int32
    questions     int32
}
```

- 在会话建立时，从 server 中获取此用户已经建立会话个数 conn，然后做判断
  - 如果全局缺省 max_user_connections = 0 && 用户 max_user_connections = 0，则检查通过；
  - 如果用户级别 max_user_connections > 0 && conn > 用户级别； max_user_connections，则检查失败；
  - 如果全局缺省 max_user_connections > 0 && conn > 全局缺省 max_user_connections，则检查失败。

### 实例级别的限制
和 mysql 的单机特性不同，TiDB 是一个分布式数据库，上述资源在单个 tidb instance 中做了维护，故此功能需要说明的是：
- 当用户修改全局变量 max_user_connections 时，所有的 tidb instance 都会生效并且持久化；
- 全局缺省限制和用户级别限制都作用于单个 tidb instance（如果限制了 user01 的连接数为 3，那么 user01 可以从 tidb-server1 建立 3 个会话，同时也可以从 tidb-server2 建立 3 个会话）。